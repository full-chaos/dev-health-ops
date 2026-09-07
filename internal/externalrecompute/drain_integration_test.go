//go:build integration

package externalrecompute

// drain_integration_test.go exercises the claim/lease protocol against a real
// PostgreSQL, because the properties that matter here are properties of
// PostgreSQL's locking, not of this package's Go: FOR UPDATE SKIP LOCKED under
// concurrency, and a lease that expires on wall-clock time. A fake store would
// assert only that the code calls the functions it calls.
//
// The Enqueuer is faked deliberately, so a failure here is unambiguously about
// claiming rather than about daily-metrics or investment publishing. The real
// enqueue path is covered end to end in enqueue_integration_test.go.

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type recordingEnqueuer struct {
	mu    sync.Mutex
	calls map[string]int
	fail  error
}

func newRecordingEnqueuer() *recordingEnqueuer {
	return &recordingEnqueuer{calls: map[string]int{}}
}

func (enqueuer *recordingEnqueuer) Enqueue(
	_ context.Context, _ pgx.Tx, plan Plan, correlation string,
) (Enqueued, error) {
	enqueuer.mu.Lock()
	defer enqueuer.mu.Unlock()
	if enqueuer.fail != nil {
		return Enqueued{}, enqueuer.fail
	}
	enqueuer.calls[correlation]++
	runIDs := make([]string, 0, plan.BackfillDays)
	for _, day := range plan.DailyTargetDays() {
		runIDs = append(runIDs, uuid.NewSHA1(uuid.NameSpaceOID,
			[]byte(correlation+day.Format(time.DateOnly))).String())
	}
	return Enqueued{
		DailyRunIDs:         runIDs,
		InvestmentRequestID: uuid.NewSHA1(uuid.NameSpaceOID, []byte(correlation)).String(),
	}, nil
}

func (enqueuer *recordingEnqueuer) total() int {
	enqueuer.mu.Lock()
	defer enqueuer.mu.Unlock()
	total := 0
	for _, count := range enqueuer.calls {
		total += count
	}
	return total
}

func (enqueuer *recordingEnqueuer) countFor(correlation string) int {
	enqueuer.mu.Lock()
	defer enqueuer.mu.Unlock()
	return enqueuer.calls[correlation]
}

func drainTestPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Postgres: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createCompatibilityTables(t, ctx, pool)
	return pool
}

// seedNativeRow writes one recompute row addressed to the native consumer plus
// the pending batch row carrying its scope, exactly as
// PostgresNativeDispatcher.Dispatch would.
func seedNativeRow(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	orgID, sourceInstance string,
	dispatchedAt time.Time,
	status string,
	mutate func(map[string]any),
) (uuid.UUID, string) {
	t.Helper()
	bridgeID := uuid.New()
	scope := map[string]any{
		"bridgeVersion":   1,
		"bridgeKind":      CompatibilityBridgeKind,
		"bridgeId":        bridgeID.String(),
		"repoIds":         []string{"11111111-2222-4333-8444-555555555555"},
		"teamIds":         []string{"team-a"},
		"recordKinds":     []string{"commit.v1"},
		"windowStartedAt": "2026-08-18T00:00:00Z",
		"windowEndedAt":   "2026-08-19T00:00:00Z",
	}
	if mutate != nil {
		mutate(scope)
	}
	encoded, err := json.Marshal(scope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO external_ingest_batches (
    ingestion_id, org_id, source_system, source_instance,
    recompute_status, recompute_scope, updated_at
) VALUES ($1,$2,'github',$3,'pending',$4,now())`,
		uuid.New(), orgID, sourceInstance, encoded); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO external_ingest_recompute_jobs (
    id, org_id, source_system, source_instance, celery_task_name,
    celery_task_id, queue, repo_id, status, dispatched_at
) VALUES ($1,$2,'github',$3,$4,$5,'default',NULL,$6,$7)`,
		bridgeID, orgID, sourceInstance, NativeDrainTaskName,
		bridgeID.String(), status, dispatchedAt); err != nil {
		t.Fatal(err)
	}
	return bridgeID, "ext-recompute:" + bridgeID.String()
}

func newTestDrain(t *testing.T, pool *pgxpool.Pool, enqueuer Enqueuer, now time.Time) *Drain {
	t.Helper()
	drain, err := NewDrain(pool, enqueuer, DefaultDrainConfig(),
		slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	drain.now = func() time.Time { return now }
	return drain
}

func rowStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) string {
	t.Helper()
	var status string
	if err := pool.QueryRow(ctx,
		"SELECT status FROM external_ingest_recompute_jobs WHERE id = $1", id).
		Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

// TestDrainClaimsEachRowExactlyOnceUnderConcurrency is the property the whole
// claim protocol exists for. Sixteen drains racing over the same rows must
// enqueue each row's work once and only once: FOR UPDATE SKIP LOCKED is what
// makes that true, and a regression to a plain SELECT-then-UPDATE would show up
// here as a duplicate enqueue rather than as a compile error.
func TestDrainClaimsEachRowExactlyOnceUnderConcurrency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := drainTestPool(t, ctx)
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)

	const rows = 24
	correlations := make([]string, 0, rows)
	ids := make([]uuid.UUID, 0, rows)
	for index := range rows {
		id, correlation := seedNativeRow(t, ctx, pool, uuid.New().String(),
			"acme/api-"+uuid.New().String()[:8], now.Add(-time.Duration(index)*time.Minute),
			statusPending, nil)
		ids = append(ids, id)
		correlations = append(correlations, correlation)
	}

	enqueuer := newRecordingEnqueuer()
	// One Drain shared by every goroutine: Step holds no mutable state of its
	// own, so the only thing serialising these racers is PostgreSQL, which is
	// exactly what this test is about.
	drain := newTestDrain(t, pool, enqueuer, now)
	var waitGroup sync.WaitGroup
	for range 16 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for range 4 {
				if _, err := drain.Step(ctx); err != nil {
					t.Errorf("step: %v", err)
					return
				}
			}
		}()
	}
	waitGroup.Wait()

	if total := enqueuer.total(); total != rows {
		t.Fatalf("enqueue calls = %d, want exactly %d", total, rows)
	}
	for _, correlation := range correlations {
		if count := enqueuer.countFor(correlation); count != 1 {
			t.Fatalf("correlation %s enqueued %d times", correlation, count)
		}
	}
	for _, id := range ids {
		if status := rowStatus(t, ctx, pool, id); status != statusDispatched {
			t.Fatalf("row %s status = %s", id, status)
		}
	}
	var pendingBatches int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM external_ingest_batches WHERE recompute_status = 'pending'").
		Scan(&pendingBatches); err != nil {
		t.Fatal(err)
	}
	if pendingBatches != 0 {
		t.Fatalf("pending batch rows = %d, want every one terminalized", pendingBatches)
	}
}

// TestDrainReclaimsAStaleClaimAndDoesNotReEnqueue covers the crash window: a
// worker that claimed a row and died before enqueuing must have that row become
// eligible again, and a worker that died AFTER enqueuing (so the batch rows are
// already terminal) must complete the row without emitting a second set of
// jobs. Both halves are what stop a crash from either losing or duplicating a
// recompute.
func TestDrainReclaimsAStaleClaimAndDoesNotReEnqueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := drainTestPool(t, ctx)
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)

	// Claimed six minutes ago: past DefaultDrainConfig's five-minute lease.
	staleID, staleCorrelation := seedNativeRow(t, ctx, pool, uuid.New().String(),
		"acme/stale", now.Add(-6*time.Minute), statusClaimed, nil)
	// Claimed one minute ago: still leased, must NOT be touched.
	freshID, freshCorrelation := seedNativeRow(t, ctx, pool, uuid.New().String(),
		"acme/fresh", now.Add(-time.Minute), statusClaimed, nil)

	enqueuer := newRecordingEnqueuer()
	drain := newTestDrain(t, pool, enqueuer, now)
	claimed, err := drain.Step(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if claimed != 1 {
		t.Fatalf("claimed = %d, want only the stale row", claimed)
	}
	if enqueuer.countFor(staleCorrelation) != 1 {
		t.Fatalf("stale row was not enqueued: %v", enqueuer.calls)
	}
	if enqueuer.countFor(freshCorrelation) != 0 {
		t.Fatal("a live lease was stolen")
	}
	if status := rowStatus(t, ctx, pool, staleID); status != statusDispatched {
		t.Fatalf("stale row status = %s", status)
	}
	if status := rowStatus(t, ctx, pool, freshID); status != statusClaimed {
		t.Fatalf("fresh row status = %s", status)
	}

	// Now simulate the crash-after-enqueue case: put the already-drained row
	// back to bridge_claimed with an expired lease. Its batch rows are terminal
	// already, so the scope lookup finds nothing and the row must complete
	// without a second enqueue.
	if _, err := pool.Exec(ctx, `
UPDATE external_ingest_recompute_jobs SET status = $2, dispatched_at = $3
WHERE id = $1`, staleID, statusClaimed, now.Add(-10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := drain.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if count := enqueuer.countFor(staleCorrelation); count != 1 {
		t.Fatalf("already-terminal row re-enqueued: count = %d", count)
	}
	if status := rowStatus(t, ctx, pool, staleID); status != statusDispatched {
		t.Fatalf("re-claimed terminal row status = %s", status)
	}
}

// TestDrainNeverClaimsLegacyCeleryRows is the structural guarantee that lets
// this port ship without a soak: the ~2.5 weeks of rows written while Celery was
// stopped carry the legacy task name, and the live consumer must be incapable of
// seeing them. If this test ever fails, deploying the worker replays the whole
// backlog on startup.
func TestDrainNeverClaimsLegacyCeleryRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := drainTestPool(t, ctx)
	now := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)

	legacyID := uuid.New()
	if _, err := pool.Exec(ctx, `
INSERT INTO external_ingest_recompute_jobs (
    id, org_id, source_system, source_instance, celery_task_name,
    celery_task_id, queue, repo_id, status, dispatched_at
) VALUES ($1,'org-1','github','acme/api',$2,$3,'default',NULL,'bridge_pending',$4)`,
		legacyID, LegacyCeleryTaskName, legacyID.String(), now.Add(-18*24*time.Hour)); err != nil {
		t.Fatal(err)
	}

	enqueuer := newRecordingEnqueuer()
	drain := newTestDrain(t, pool, enqueuer, now)
	claimed, err := drain.Step(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if claimed != 0 || enqueuer.total() != 0 {
		t.Fatalf("claimed = %d enqueued = %d, want the legacy row untouched",
			claimed, enqueuer.total())
	}
	if status := rowStatus(t, ctx, pool, legacyID); status != statusPending {
		t.Fatalf("legacy row status = %s, want it left for the replay command", status)
	}
}

// TestDrainRetiresAPermanentlyRejectedPayload pins the failure classification.
// A payload no retry can fix must terminalize BOTH the recompute row and the
// batch rows it covered: marking only the recompute row would leave those
// batches reporting recompute_status='pending' forever, since nothing would ever
// claim them again.
func TestDrainRetiresAPermanentlyRejectedPayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := drainTestPool(t, ctx)
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)

	id, _ := seedNativeRow(t, ctx, pool, uuid.New().String(), "acme/bad",
		now.Add(-time.Minute), statusPending, func(scope map[string]any) {
			scope["bridgeVersion"] = 2
		})

	enqueuer := newRecordingEnqueuer()
	drain := newTestDrain(t, pool, enqueuer, now)
	if _, err := drain.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if enqueuer.total() != 0 {
		t.Fatal("a rejected payload must enqueue nothing")
	}
	if status := rowStatus(t, ctx, pool, id); status != statusFailed {
		t.Fatalf("row status = %s", status)
	}
	var batchStatus string
	var batchError *string
	if err := pool.QueryRow(ctx, `
SELECT recompute_status, recompute_error FROM external_ingest_batches LIMIT 1`).
		Scan(&batchStatus, &batchError); err != nil {
		t.Fatal(err)
	}
	if batchStatus != outcomeFailed {
		t.Fatalf("batch status = %s, want the batch retired with the row", batchStatus)
	}
	if batchError == nil || *batchError == "" {
		t.Fatal("batch carries no error text, so the rejection is invisible to the status API")
	}
}

// TestDrainLeavesATransientFailureClaimable is the other half of the
// classification: an enqueue that could succeed later must NOT be marked
// terminal, so the stale lease brings it back.
func TestDrainLeavesATransientFailureClaimable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := drainTestPool(t, ctx)
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)

	id, correlation := seedNativeRow(t, ctx, pool, uuid.New().String(), "acme/flaky",
		now.Add(-time.Minute), statusPending, nil)

	enqueuer := newRecordingEnqueuer()
	enqueuer.fail = context.DeadlineExceeded
	drain := newTestDrain(t, pool, enqueuer, now)
	if _, err := drain.Step(ctx); err == nil {
		t.Fatal("a failing enqueue must surface an error")
	}
	if status := rowStatus(t, ctx, pool, id); status != statusClaimed {
		t.Fatalf("row status = %s, want it left claimed for the lease to reclaim", status)
	}

	enqueuer.fail = nil
	later := now.Add(6 * time.Minute)
	recovered := newTestDrain(t, pool, enqueuer, later)
	if _, err := recovered.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if enqueuer.countFor(correlation) != 1 {
		t.Fatalf("recovered enqueue count = %d", enqueuer.countFor(correlation))
	}
	if status := rowStatus(t, ctx, pool, id); status != statusDispatched {
		t.Fatalf("row status = %s", status)
	}
}

// TestReplayCollapsesTheLegacyBacklogAndRetiresIt drives the operator command's
// engine against real rows: the backlog collapses to one plan per grain, every
// row is retired, and a dry run changes nothing.
func TestReplayCollapsesTheLegacyBacklogAndRetiresIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := drainTestPool(t, ctx)
	now := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)

	orgID := uuid.New().String()
	legacyIDs := make([]uuid.UUID, 0, 3)
	for day := range 3 {
		bridgeID := uuid.New()
		scope, err := json.Marshal(map[string]any{
			"bridgeVersion":   1,
			"bridgeKind":      CompatibilityBridgeKind,
			"bridgeId":        bridgeID.String(),
			"repoIds":         []string{uuid.New().String()},
			"recordKinds":     []string{"commit.v1"},
			"windowStartedAt": now.AddDate(0, 0, -18+day).Format(time.RFC3339),
			"windowEndedAt":   now.AddDate(0, 0, -18+day).Format(time.RFC3339),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO external_ingest_batches (
    ingestion_id, org_id, source_system, source_instance,
    recompute_status, recompute_scope, updated_at
) VALUES ($1,$2,'github','acme/api','pending',$3,now())`,
			uuid.New(), orgID, scope); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO external_ingest_recompute_jobs (
    id, org_id, source_system, source_instance, celery_task_name,
    celery_task_id, queue, repo_id, status, dispatched_at
) VALUES ($1,$2,'github','acme/api',$3,$4,'default',NULL,'bridge_pending',$5)`,
			bridgeID, orgID, LegacyCeleryTaskName, bridgeID.String(),
			now.AddDate(0, 0, -18+day)); err != nil {
			t.Fatal(err)
		}
		legacyIDs = append(legacyIDs, bridgeID)
	}

	enqueuer := newRecordingEnqueuer()
	dry, err := Replay(ctx, pool, enqueuer, now, 100, true)
	if err != nil {
		t.Fatal(err)
	}
	if dry.Rows != 3 || len(dry.Groups) != 1 || dry.Retired != 0 {
		t.Fatalf("dry run report = %+v", dry)
	}
	if !dry.Groups[0].Trigger || dry.Groups[0].RepoIDs != 3 {
		t.Fatalf("dry run group = %+v, want the union of all three rows", dry.Groups[0])
	}
	if enqueuer.total() != 0 {
		t.Fatal("a dry run must not enqueue")
	}
	for _, id := range legacyIDs {
		if status := rowStatus(t, ctx, pool, id); status != statusPending {
			t.Fatalf("dry run mutated row %s to %s", id, status)
		}
	}

	report, err := Replay(ctx, pool, enqueuer, now, 100, false)
	if err != nil {
		t.Fatal(err)
	}
	if report.Retired != 3 || report.Failed != 0 || len(report.Groups) != 1 {
		t.Fatalf("replay report = %+v", report)
	}
	// One collapsed plan, not one per row: replaying each row individually is
	// exactly what the collapse exists to avoid.
	if enqueuer.total() != 1 {
		t.Fatalf("enqueue calls = %d, want one per collapsed grain", enqueuer.total())
	}
	for _, id := range legacyIDs {
		if status := rowStatus(t, ctx, pool, id); status != statusDispatched {
			t.Fatalf("row %s status = %s", id, status)
		}
	}
	// Re-running must be a no-op, so an operator who runs it twice does not
	// enqueue a second set.
	again, err := Replay(ctx, pool, enqueuer, now, 100, false)
	if err != nil {
		t.Fatal(err)
	}
	if again.Rows != 0 || enqueuer.total() != 1 {
		t.Fatalf("second replay was not a no-op: %+v (enqueues %d)", again, enqueuer.total())
	}
}
