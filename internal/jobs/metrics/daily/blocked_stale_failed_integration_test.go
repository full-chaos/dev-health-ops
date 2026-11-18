//go:build integration

package daily

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

type stalePartition struct {
	status    string
	updatedAt time.Time
}

func seedStaleRun(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, runID string,
	runAt time.Time, partitions []stalePartition,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_runs (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at) VALUES ($1::uuid,$2,'2026-09-17','daily-v1-' || $1::uuid::text,'running','pending',$3,$3)`,
		runID, orgID, runAt); err != nil {
		t.Fatal(err)
	}
	for ordinal, partition := range partitions {
		var reason, token, lease any
		switch partition.status {
		case "failed_permanent":
			reason = "attempts_exhausted"
		case runningLiveLease:
			token, lease = uuid.NewString(), partition.updatedAt.Add(48*time.Hour)
		}
		status := partition.status
		if status == runningLiveLease {
			status = "running"
		}
		if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_partitions (id,run_id,ordinal,repo_ids,status,attempt_count,created_at,updated_at,failure_reason,claim_token,lease_expires_at) VALUES ($1,$2,$3,'[]'::jsonb,$4,1,$5,$5,$6,$7::uuid,$8)`,
			uuid.NewString(), runID, ordinal, status, partition.updatedAt, reason, token, lease); err != nil {
			t.Fatalf("seeding partition %d %s: %v", ordinal, partition.status, err)
		}
	}
}

// A 'failed' partition is claimed again only by the retry of its River job.
// Once that job is gone nothing re-publishes it: prod held 9 such runs on
// 2026-11-18, the oldest 1493 h old, none marked. Past
// failedPartitionRetryHorizon the run can never finish and the marker must see
// it; inside the horizon a retry can still claim it.
func TestReconcileBlockedRunsMarksAFailedPartitionPastTheRetryHorizon(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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

	orgID := uuid.NewString()
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	stale := now.Add(-failedPartitionRetryHorizon - time.Minute)
	atHorizon := now.Add(-failedPartitionRetryHorizon)
	fresh := now.Add(-time.Hour)

	for _, testCase := range []struct {
		name        string
		partitions  []stalePartition
		wantBlocked bool
		wantReason  string
	}{
		{
			name:        "a failed partition past the horizon is blocked",
			partitions:  []stalePartition{{"failed", stale}},
			wantBlocked: true,
			wantReason:  BlockedReasonPartitionsFailedNoRetry,
		},
		{
			name:        "a failed partition exactly at the horizon is blocked",
			partitions:  []stalePartition{{"failed", atHorizon}},
			wantBlocked: true,
			wantReason:  BlockedReasonPartitionsFailedNoRetry,
		},
		{
			name:        "succeeded alongside a failed partition past the horizon is blocked",
			partitions:  []stalePartition{{"succeeded", stale}, {"failed", stale}},
			wantBlocked: true,
			wantReason:  BlockedReasonPartitionsFailedNoRetry,
		},
		{
			// A failed_permanent row keeps its own reason: the operator reads
			// its failure_reason first.
			name:        "failed_permanent alongside a failed partition past the horizon is blocked",
			partitions:  []stalePartition{{"failed_permanent", stale}, {"failed", stale}},
			wantBlocked: true,
			wantReason:  BlockedReasonAllPartitionsPermanent,
		},
		{
			name:        "succeeded and failed_permanent alongside a stale failed partition is partial",
			partitions:  []stalePartition{{"succeeded", stale}, {"failed_permanent", stale}, {"failed", stale}},
			wantBlocked: true,
			wantReason:  BlockedReasonPartialPartitionsPermanent,
		},
		{
			name:        "a failed partition inside the horizon is not blocked",
			partitions:  []stalePartition{{"failed", fresh}},
			wantBlocked: false,
		},
		{
			name:        "a stale failed partition alongside a fresh failed one is not blocked",
			partitions:  []stalePartition{{"failed", stale}, {"failed", fresh}},
			wantBlocked: false,
		},
		{
			name:        "a stale failed partition alongside a pending one is not blocked",
			partitions:  []stalePartition{{"failed", stale}, {"pending", stale}},
			wantBlocked: false,
		},
		{
			name:        "a stale failed partition alongside a live-lease running one is not blocked",
			partitions:  []stalePartition{{"failed", stale}, {runningLiveLease, now}},
			wantBlocked: false,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runID := uuid.NewString()
			seedStaleRun(t, ctx, pool, orgID, runID, stale, testCase.partitions)
			store, err := NewPostgresStore(pool)
			if err != nil {
				t.Fatal(err)
			}
			store.now = func() time.Time { return now }
			if _, err := store.ReconcileBlockedRuns(ctx, orgID); err != nil {
				t.Fatal(err)
			}
			blocked, reason := readMarker(t, ctx, pool, runID)
			if blocked != testCase.wantBlocked {
				t.Fatalf("blocked = %t, want %t", blocked, testCase.wantBlocked)
			}
			if blocked && reason != testCase.wantReason {
				t.Fatalf("reason = %q, want %q", reason, testCase.wantReason)
			}
			if !blocked || testCase.wantReason != BlockedReasonPartitionsFailedNoRetry {
				return
			}
			// Reversibility: a write to the failed row (a claim, a release, a
			// redrive reset) brings it inside the horizon, and the next pass
			// clears the marker.
			if _, err := pool.Exec(ctx, `UPDATE daily_metrics_partitions SET updated_at = $2 WHERE run_id = $1 AND status = 'failed'`, runID, now); err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReconcileBlockedRuns(ctx, orgID); err != nil {
				t.Fatal(err)
			}
			if stillBlocked, _ := readMarker(t, ctx, pool, runID); stillBlocked {
				t.Fatal("the marker stayed after the failed partition was written again")
			}
		})
	}
}

// The redrive and the marker meet on 'failed': step 1 of a redrive resets
// failed_permanent to 'failed' with updated_at = now, and that row must not be
// marked again. A stale-'failed' run stays marked through the redrive (step 2
// publishes, it does not write the row) until the redriven job claims it.
func TestRedrivenFailedPartitionsAndTheRetryHorizon(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	stale := now.Add(-failedPartitionRetryHorizon - time.Hour)

	orgID := uuid.NewString()
	permanentRun, staleFailedRun := uuid.NewString(), uuid.NewString()
	seedStaleRun(t, ctx, pool, orgID, permanentRun, stale, []stalePartition{{"succeeded", stale}, {"failed_permanent", stale}})
	seedStaleRun(t, ctx, pool, orgID, staleFailedRun, stale, []stalePartition{{"succeeded", stale}, {"failed", stale}})
	if _, err := store.ReconcileBlockedRuns(ctx, orgID); err != nil {
		t.Fatal(err)
	}
	for runID, want := range map[string]string{
		permanentRun:   BlockedReasonPartialPartitionsPermanent,
		staleFailedRun: BlockedReasonPartitionsFailedNoRetry,
	} {
		if blocked, reason := readMarker(t, ctx, pool, runID); !blocked || reason != want {
			t.Fatalf("precondition %s: blocked=%t reason=%q, want %q", runID, blocked, reason, want)
		}
	}
	listed, err := store.BlockedRuns(ctx, orgID, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range listed {
		if row.RunID == staleFailedRun && (row.FailedPartitions != 1 || row.PermanentPartitions != 0 || row.SucceededPartitions != 1) {
			t.Fatalf("readback of the stale-failed run = %+v, want 1 failed, 0 failed_permanent, 1 succeeded", row)
		}
	}

	outcome, err := store.RedriveStrandedPartitions(ctx, publisher, orgID,
		time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if outcome.PermanentReset != 1 || outcome.RedrivenPartitions != 2 {
		t.Fatalf("redrive outcome = %+v, want 1 reset and 2 redriven partitions", outcome)
	}
	if _, err := store.ReconcileBlockedRuns(ctx, orgID); err != nil {
		t.Fatal(err)
	}
	if blocked, _ := readMarker(t, ctx, pool, permanentRun); blocked {
		t.Fatal("a partition the redrive just reset to 'failed' was marked again")
	}
	if blocked, reason := readMarker(t, ctx, pool, staleFailedRun); !blocked || reason != BlockedReasonPartitionsFailedNoRetry {
		t.Fatalf("stale-failed run after the redrive: blocked=%t reason=%q, want still marked until its claim", blocked, reason)
	}

	// The redriven job claims the partition: a live lease, and the next pass
	// clears the marker.
	var partitionID string
	if err := pool.QueryRow(ctx, `SELECT id::text FROM daily_metrics_partitions WHERE run_id = $1 AND status = 'failed'`, staleFailedRun).Scan(&partitionID); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimPartition(ctx, partitionID)
	if err != nil || claim == nil {
		t.Fatalf("ClaimPartition: %v %v", claim, err)
	}
	if _, err := store.ReconcileBlockedRuns(ctx, orgID); err != nil {
		t.Fatal(err)
	}
	if blocked, _ := readMarker(t, ctx, pool, staleFailedRun); blocked {
		t.Fatal("the marker stayed after the redriven job claimed the partition")
	}
}

func readMarkerState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string) (*time.Time, string, time.Time) {
	t.Helper()
	var blockedAt *time.Time
	var reason *string
	var updatedAt time.Time
	if err := pool.QueryRow(ctx, `SELECT blocked_at, blocked_reason, updated_at FROM daily_metrics_runs WHERE id = $1`, runID).
		Scan(&blockedAt, &reason, &updatedAt); err != nil {
		t.Fatal(err)
	}
	if reason == nil {
		return blockedAt, "", updatedAt
	}
	return blockedAt, *reason, updatedAt
}

// A run that stays blocked while its partitions change must carry the reason
// of its current partitions. A redriven stale 'failed' partition can be claimed
// and end failed_permanent, or succeed beside a failed_permanent one, before the
// next pass; the marker's blocked_at and the marked counter stay as they were.
func TestBlockedReasonFollowsThePartitionsWhileTheRunStaysBlocked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
	firstPass := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	stale := firstPass.Add(-failedPartitionRetryHorizon - time.Hour)

	for _, testCase := range []struct {
		name       string
		partitions []stalePartition
		firstWant  string
		settle     func(t *testing.T, claim PartitionClaim)
		secondWant string
	}{
		{
			name:       "a stale failed partition that ends failed_permanent",
			partitions: []stalePartition{{"failed", stale}},
			firstWant:  BlockedReasonPartitionsFailedNoRetry,
			settle: func(t *testing.T, claim PartitionClaim) {
				if err := store.FailPartitionPermanently(ctx, claim, "attempts_exhausted"); err != nil {
					t.Fatal(err)
				}
			},
			secondWant: BlockedReasonAllPartitionsPermanent,
		},
		{
			name:       "a stale failed partition that ends failed_permanent beside a success",
			partitions: []stalePartition{{"succeeded", stale}, {"failed", stale}},
			firstWant:  BlockedReasonPartitionsFailedNoRetry,
			settle: func(t *testing.T, claim PartitionClaim) {
				if err := store.FailPartitionPermanently(ctx, claim, "attempts_exhausted"); err != nil {
					t.Fatal(err)
				}
			},
			secondWant: BlockedReasonPartialPartitionsPermanent,
		},
		{
			name:       "a stale failed partition that succeeds beside a failed_permanent one",
			partitions: []stalePartition{{"failed_permanent", stale}, {"failed", stale}},
			firstWant:  BlockedReasonAllPartitionsPermanent,
			settle: func(t *testing.T, claim PartitionClaim) {
				if err := store.CompletePartition(ctx, claim, publisher); err != nil {
					t.Fatal(err)
				}
			},
			secondWant: BlockedReasonPartialPartitionsPermanent,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			orgID, runID := uuid.NewString(), uuid.NewString()
			seedStaleRun(t, ctx, pool, orgID, runID, stale, testCase.partitions)
			store.now = func() time.Time { return firstPass }
			if _, err := store.ReconcileBlockedRuns(ctx, orgID); err != nil {
				t.Fatal(err)
			}
			markedAt, reason, _ := readMarkerState(t, ctx, pool, runID)
			if markedAt == nil || reason != testCase.firstWant {
				t.Fatalf("first pass: blocked_at=%v reason=%q, want marked %q", markedAt, reason, testCase.firstWant)
			}

			// Between two nightly passes: the redriven job claims the stale row
			// and settles it.
			between := firstPass.Add(time.Hour)
			store.now = func() time.Time { return between }
			var partitionID string
			if err := pool.QueryRow(ctx, `SELECT id::text FROM daily_metrics_partitions WHERE run_id = $1 AND status = 'failed'`, runID).Scan(&partitionID); err != nil {
				t.Fatal(err)
			}
			claim, err := store.ClaimPartition(ctx, partitionID)
			if err != nil || claim == nil {
				t.Fatalf("ClaimPartition: %v %v", claim, err)
			}
			testCase.settle(t, *claim)

			secondPass := firstPass.Add(24 * time.Hour)
			store.now = func() time.Time { return secondPass }
			outcome, err := store.ReconcileBlockedRuns(ctx, orgID)
			if err != nil {
				t.Fatal(err)
			}
			blockedAt, reason, updatedAt := readMarkerState(t, ctx, pool, runID)
			if blockedAt == nil || reason != testCase.secondWant {
				t.Fatalf("second pass: blocked_at=%v reason=%q, want marked %q", blockedAt, reason, testCase.secondWant)
			}
			if !blockedAt.Equal(*markedAt) {
				t.Fatalf("blocked_at moved from %s to %s on a reason change", markedAt, blockedAt)
			}
			if outcome.Marked != 0 || outcome.Cleared != 0 || outcome.Blocked != 1 {
				t.Fatalf("second pass outcome = %+v, want 0 marked, 0 cleared, 1 blocked", outcome)
			}

			// A steady pass writes nothing.
			store.now = func() time.Time { return secondPass.Add(time.Hour) }
			if _, err := store.ReconcileBlockedRuns(ctx, orgID); err != nil {
				t.Fatal(err)
			}
			if _, _, steady := readMarkerState(t, ctx, pool, runID); !steady.Equal(updatedAt) {
				t.Fatalf("a steady pass wrote the run: updated_at %s -> %s", updatedAt, steady)
			}
		})
	}
}

func waitForLockWaiters(t *testing.T, ctx context.Context, pool *pgxpool.Pool, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND state = 'active' AND wait_event_type = 'Lock'`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("fewer than %d backends blocked on a lock", want)
}

// A reason-only correction must not restore a marker that a concurrent redrive
// cleared. The pass reads the run as blocked with an out-of-date reason; the
// redrive resets the failed_permanent partition to a fresh 'failed' row and
// clears the marker in its own transaction; the pass's UPDATE waits on that run
// row and, under Read Committed, re-checks only its WHERE against the committed
// row. The interleaving is forced: an uncommitted outbox row under the
// redrive's own dedupe key holds the redrive open after it cleared the marker.
func TestReasonCorrectionDoesNotRestoreAMarkerARedriveCleared(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
	firstPass := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	stale := firstPass.Add(-failedPartitionRetryHorizon - time.Hour)
	orgID, runID := uuid.NewString(), uuid.NewString()
	seedStaleRun(t, ctx, pool, orgID, runID, stale, []stalePartition{{"failed_permanent", stale}, {"failed", stale}})
	store.now = func() time.Time { return firstPass }
	if _, err := store.ReconcileBlockedRuns(ctx, orgID); err != nil {
		t.Fatal(err)
	}
	if _, reason, _ := readMarkerState(t, ctx, pool, runID); reason != BlockedReasonAllPartitionsPermanent {
		t.Fatalf("first pass reason = %q, want %q", reason, BlockedReasonAllPartitionsPermanent)
	}

	// The stale row is claimed and succeeds: the next pass wants the partial reason.
	store.now = func() time.Time { return firstPass.Add(time.Hour) }
	var staleID, permanentID string
	if err := pool.QueryRow(ctx, `SELECT id::text FROM daily_metrics_partitions WHERE run_id = $1 AND status = 'failed'`, runID).Scan(&staleID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT id::text FROM daily_metrics_partitions WHERE run_id = $1 AND status = 'failed_permanent'`, runID).Scan(&permanentID); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimPartition(ctx, staleID)
	if err != nil || claim == nil {
		t.Fatalf("ClaimPartition: %v %v", claim, err)
	}
	if err := store.CompletePartition(ctx, *claim, publisher); err != nil {
		t.Fatal(err)
	}

	secondPass := firstPass.Add(24 * time.Hour)
	store.now = func() time.Time { return secondPass }
	nonce := uuid.NewString()
	day := time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC)

	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(context.Background()) }()
	if err := publisher.PublishRedrivePartitionTx(ctx, blocker,
		Run{ID: runID, OrganizationID: orgID, Status: "running"},
		Partition{ID: permanentID, RunID: runID}, nonce); err != nil {
		t.Fatal(err)
	}

	redriveDone := make(chan error, 1)
	go func() {
		_, err := store.RedriveStrandedPartitions(ctx, publisher, orgID, day, day, nonce)
		redriveDone <- err
	}()
	waitForLockWaiters(t, ctx, pool, 1)
	reconcileDone := make(chan error, 1)
	go func() {
		_, err := store.ReconcileBlockedRuns(ctx, orgID)
		reconcileDone <- err
	}()
	waitForLockWaiters(t, ctx, pool, 2)
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-redriveDone; err != nil {
		t.Fatalf("redrive: %v", err)
	}
	if err := <-reconcileDone; err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var partitionStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM daily_metrics_partitions WHERE id = $1`, permanentID).Scan(&partitionStatus); err != nil {
		t.Fatal(err)
	}
	if partitionStatus != "failed" {
		t.Fatalf("precondition: the redrive left the partition %q, want failed", partitionStatus)
	}
	if blockedAt, reason, _ := readMarkerState(t, ctx, pool, runID); blockedAt != nil {
		t.Fatalf("the pass restored the marker the redrive cleared: blocked_at=%s reason=%q", blockedAt, reason)
	}
	if _, err := store.ReconcileBlockedRuns(ctx, orgID); err != nil {
		t.Fatal(err)
	}
	if blockedAt, reason, _ := readMarkerState(t, ctx, pool, runID); blockedAt != nil {
		t.Fatalf("the next pass marked a run with a fresh failed partition: reason=%q", reason)
	}
}
