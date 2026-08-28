//go:build integration

package daily

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-4405, team-lead escalation 2026-08-28 (the residual silent-exclusion
// gap): a finalize-redrive event stays 'open' forever if the River job it
// published is discarded/cancelled, or never reaches River at all, BEFORE
// ClaimFinalize is ever called for it -- neither CompleteFinalize/
// ReleaseFinalize nor transitionFinalize's own 'closed_failed' close-out can
// ever see this shape, since no claim happens at all. The two tests below
// exercise both orphan shapes named in the ruling: the relay-level 'dead'
// outbox row (never reached River), and a River job the queue itself
// discarded. Checked out against the commit before
// ReconcileOrphanedFinalizeRedriveRuns existed, neither test compiles --
// the gap is that no existing Store capability can ever close this row at
// all, not merely that some existing call returns the wrong thing.

// TestReconcileOrphanedFinalizeRedriveRunsClosesAnEventWhenTheOutboxRowNeverReachedRiver
// is the easier orphan sub-case: the relay itself (internal/joboutbox/
// repository.go) exhausted its own delivery attempts and marked the outbox
// row 'dead' before the job ever reached River. This needs no River schema
// at all -- the existing lightweight test stack is enough.
func TestReconcileOrphanedFinalizeRedriveRunsClosesAnEventWhenTheOutboxRowNeverReachedRiver(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newFinalizeRedriveTestStack(t)
	var redriveObservations []struct {
		outcome string
		count   int
	}
	store.SetFinalizeRedriveObserver(recordingFinalizeRedriveObserver{sink: &redriveObservations})

	const (
		orgID       = "00000000-0000-4000-8000-000000002701"
		runID       = "00000000-0000-4000-8000-000000002702"
		partitionID = "00000000-0000-4000-8000-000000002703"
	)
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	targetDay := time.Date(2026, 5, 7, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, runID, orgID, targetDay, now)
	insertFinalizeTestPartition(t, ctx, pool, partitionID, runID, 0, "succeeded", now)
	if _, err := pool.Exec(ctx, `
UPDATE daily_metrics_runs
SET status = 'succeeded', finalization_status = 'succeeded', finalized_at = $1
WHERE id = $2::uuid`, now, runID); err != nil {
		t.Fatal(err)
	}

	outcome, err := store.RedriveFinalizeForRange(
		ctx, publisher, orgID, targetDay, targetDay, "orphan-nonce-dead", true, testFinalizeRedriveReason, false,
	)
	if err != nil {
		t.Fatalf("RedriveFinalizeForRange: %v", err)
	}
	if len(outcome.Days) != 1 || outcome.Days[0].Outcome != "redriven_reset_from_succeeded" {
		t.Fatalf("RedriveFinalizeForRange outcome = %#v, want redriven_reset_from_succeeded", outcome)
	}

	// Simulate the redriven job never reaching River at all: the relay
	// exhausted its own delivery attempts and marked the outbox row dead.
	// Never call ClaimFinalize for it -- the run is now stuck exactly like
	// any other CHAOS-4389 discard, EXCEPT its 'open' redrive event still
	// permanently excludes it from FindStrandedFinalizeRuns (RED baseline).
	if _, err := pool.Exec(ctx, `UPDATE worker_job_outbox SET status = 'dead' WHERE dedupe_key = $1`,
		"metrics.daily_finalize:redrive:"+runID+":orphan-nonce-dead"); err != nil {
		t.Fatal(err)
	}

	strandedBefore, err := store.FindStrandedFinalizeRuns(ctx, 0)
	if err != nil {
		t.Fatalf("FindStrandedFinalizeRuns (before reconcile): %v", err)
	}
	for _, id := range strandedBefore {
		if id == runID {
			t.Fatalf("FindStrandedFinalizeRuns reported %s before reconciliation ran -- RED baseline setup is wrong", runID)
		}
	}

	closed, err := store.ReconcileOrphanedFinalizeRedriveRuns(ctx, nil, "river")
	if err != nil {
		t.Fatalf("ReconcileOrphanedFinalizeRedriveRuns: %v", err)
	}
	if closed != 1 {
		t.Fatalf("ReconcileOrphanedFinalizeRedriveRuns closed = %d, want 1", closed)
	}

	var eventStatus string
	var closedAt *time.Time
	if err := pool.QueryRow(ctx, `
SELECT status, closed_at FROM daily_metrics_finalize_redrive_events
WHERE run_id = $1::uuid ORDER BY created_at LIMIT 1`, runID).
		Scan(&eventStatus, &closedAt); err != nil {
		t.Fatal(err)
	}
	if eventStatus != "closed_orphaned" || closedAt == nil {
		t.Fatalf("redrive event after reconciliation = status=%q closed_at=%v, want closed_orphaned with a timestamp", eventStatus, closedAt)
	}

	strandedAfter, err := store.FindStrandedFinalizeRuns(ctx, 0)
	if err != nil {
		t.Fatalf("FindStrandedFinalizeRuns (after reconcile): %v", err)
	}
	found := false
	for _, id := range strandedAfter {
		if id == runID {
			found = true
		}
	}
	if !found {
		t.Fatalf("FindStrandedFinalizeRuns = %v, want it to include %s once its orphaned redrive event closed", strandedAfter, runID)
	}

	var orphanedCount int
	for _, observation := range redriveObservations {
		if observation.outcome == "redriven_orphaned" {
			orphanedCount += observation.count
		}
	}
	if orphanedCount != 1 {
		t.Fatalf("redriven_orphaned telemetry observations = %d (all: %#v), want 1", orphanedCount, redriveObservations)
	}

	// Calling it again is a no-op: the event is no longer 'open'.
	closedAgain, err := store.ReconcileOrphanedFinalizeRedriveRuns(ctx, nil, "river")
	if err != nil {
		t.Fatalf("ReconcileOrphanedFinalizeRedriveRuns (second call): %v", err)
	}
	if closedAgain != 0 {
		t.Fatalf("ReconcileOrphanedFinalizeRedriveRuns (second call) closed = %d, want 0", closedAgain)
	}
}

// TestReconcileOrphanedFinalizeRedriveRunsClosesAnEventWhenRiverDiscardedTheJob
// is the harder orphan sub-case: the outbox row WAS delivered into a real
// River job, but River discarded it before a worker ever reached
// ClaimFinalize. This needs a real river.river_job table, provisioned via
// riverstore.ApplyPinnedMigrations against the SAME admin pool
// createDailyTables already used -- a superuser connection bypasses the
// least-privilege domain/queue-control split that matters in production but
// not for this test's purpose (no separate queue-control pool/role needed).
func TestReconcileOrphanedFinalizeRedriveRunsClosesAnEventWhenRiverDiscardedTheJob(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newFinalizeRedriveTestStackWithRiverSchema(t)
	var redriveObservations []struct {
		outcome string
		count   int
	}
	store.SetFinalizeRedriveObserver(recordingFinalizeRedriveObserver{sink: &redriveObservations})

	const (
		orgID       = "00000000-0000-4000-8000-000000002801"
		runID       = "00000000-0000-4000-8000-000000002802"
		partitionID = "00000000-0000-4000-8000-000000002803"
	)
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	targetDay := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, runID, orgID, targetDay, now)
	insertFinalizeTestPartition(t, ctx, pool, partitionID, runID, 0, "succeeded", now)
	if _, err := pool.Exec(ctx, `
UPDATE daily_metrics_runs
SET status = 'succeeded', finalization_status = 'succeeded', finalized_at = $1
WHERE id = $2::uuid`, now, runID); err != nil {
		t.Fatal(err)
	}

	outcome, err := store.RedriveFinalizeForRange(
		ctx, publisher, orgID, targetDay, targetDay, "orphan-nonce-discarded", true, testFinalizeRedriveReason, false,
	)
	if err != nil {
		t.Fatalf("RedriveFinalizeForRange: %v", err)
	}
	if len(outcome.Days) != 1 || outcome.Days[0].Outcome != "redriven_reset_from_succeeded" {
		t.Fatalf("RedriveFinalizeForRange outcome = %#v, want redriven_reset_from_succeeded", outcome)
	}

	// Simulate the redriven job actually reaching River, then River
	// discarding it -- e.g. the worker fleet's own attempt budget exhausted
	// before ClaimFinalize was ever called. Insert a real river.river_job
	// row directly in state='discarded' (mirrors internal/joboutbox/
	// relay_integration_test.go's own working direct-insert template), then
	// link it to the outbox row exactly the way a real delivery would
	// (status='delivered', river_job_id set -- matching
	// ck_worker_job_outbox_delivery_state).
	var riverJobID int64
	if err := pool.QueryRow(ctx, `
INSERT INTO river.river_job (
    args, attempt, created_at, errors, finalized_at, kind, max_attempts,
    metadata, priority, queue, state, scheduled_at
)
VALUES (
    '{}'::jsonb, 1, $1::timestamptz,
    ARRAY[jsonb_build_object('at', $1::timestamptz, 'attempt', 1, 'error', 'test: discarded before claim', 'trace', '')],
    $1::timestamptz, 'metrics.daily_finalize', 5,
    '{}'::jsonb, 1, 'metrics', 'discarded', $1::timestamptz
)
RETURNING id`, now).Scan(&riverJobID); err != nil {
		t.Fatal(err)
	}
	// The hand-rolled test DDL (createDailyTables) is a simplified subset of
	// the real worker_job_outbox migration and does not carry delivered_at
	// (or several other columns the real schema's delivery-state check
	// constraint pins) -- setting status/river_job_id is all this function
	// reads.
	dedupeKey := "metrics.daily_finalize:redrive:" + runID + ":orphan-nonce-discarded"
	if _, err := pool.Exec(ctx, `
UPDATE worker_job_outbox
SET status = 'delivered', river_job_id = $1
WHERE dedupe_key = $2`, riverJobID, dedupeKey); err != nil {
		t.Fatal(err)
	}

	strandedBefore, err := store.FindStrandedFinalizeRuns(ctx, 0)
	if err != nil {
		t.Fatalf("FindStrandedFinalizeRuns (before reconcile): %v", err)
	}
	for _, id := range strandedBefore {
		if id == runID {
			t.Fatalf("FindStrandedFinalizeRuns reported %s before reconciliation ran -- RED baseline setup is wrong", runID)
		}
	}

	closed, err := store.ReconcileOrphanedFinalizeRedriveRuns(ctx, pool, "river")
	if err != nil {
		t.Fatalf("ReconcileOrphanedFinalizeRedriveRuns: %v", err)
	}
	if closed != 1 {
		t.Fatalf("ReconcileOrphanedFinalizeRedriveRuns closed = %d, want 1", closed)
	}

	var eventStatus string
	var closedAt *time.Time
	if err := pool.QueryRow(ctx, `
SELECT status, closed_at FROM daily_metrics_finalize_redrive_events
WHERE run_id = $1::uuid ORDER BY created_at LIMIT 1`, runID).
		Scan(&eventStatus, &closedAt); err != nil {
		t.Fatal(err)
	}
	if eventStatus != "closed_orphaned" || closedAt == nil {
		t.Fatalf("redrive event after reconciliation = status=%q closed_at=%v, want closed_orphaned with a timestamp", eventStatus, closedAt)
	}

	strandedAfter, err := store.FindStrandedFinalizeRuns(ctx, 0)
	if err != nil {
		t.Fatalf("FindStrandedFinalizeRuns (after reconcile): %v", err)
	}
	found := false
	for _, id := range strandedAfter {
		if id == runID {
			found = true
		}
	}
	if !found {
		t.Fatalf("FindStrandedFinalizeRuns = %v, want it to include %s once its orphaned redrive event closed", strandedAfter, runID)
	}

	var orphanedCount int
	for _, observation := range redriveObservations {
		if observation.outcome == "redriven_orphaned" {
			orphanedCount += observation.count
		}
	}
	if orphanedCount != 1 {
		t.Fatalf("redriven_orphaned telemetry observations = %d (all: %#v), want 1", orphanedCount, redriveObservations)
	}
}

// riverSchemaTestDomainRole/riverSchemaTestQueueRole are dedicated,
// deliberately distinct role names for the ApplyPinnedMigrations preflight
// (ValidateMigrationOptions rejects DomainRole == QueueRole). Nothing in
// these tests actually connects through either role -- the admin pool
// (superuser) is reused for every read/write, matching newFinalizeRedrive
// TestStackWithRiverSchema's own doc comment.
const (
	riverSchemaTestDomainRole     = "finalize_redrive_test_domain"
	riverSchemaTestQueueRole      = "finalize_redrive_test_queue"
	riverSchemaTestDomainPassword = "finalize_redrive_test_domain_password"
	riverSchemaTestQueuePassword  = "finalize_redrive_test_queue_password"
)

// newFinalizeRedriveTestStackWithRiverSchema is newFinalizeRedriveTestStack
// plus a REAL river.river_job table, provisioned by
// riverstore.ApplyPinnedMigrations against the same admin pool. The admin
// pool doubles as this test's "queue-control pool" -- a superuser connection
// bypasses the least-privilege domain/queue-control split that matters in
// production but not here (see internal/joboutbox/relay_integration_test.go
// for the same pattern with a real queue-role pool, which this test does not
// need). CoordinatorRole is left empty: ValidateMigrationOptions accepts an
// empty coordinator role with no coordinator grants, and this test never
// touches coordinator-scoped behavior.
func newFinalizeRedriveTestStackWithRiverSchema(t *testing.T) (*pgxpool.Pool, *PostgresStore, *PostgresPublisher) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { instance.Close(context.Background()) })
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	// ApplyPinnedMigrations refuses a pool with MaxConns < 2 (it acquires a
	// dedicated migration-lock connection alongside its own transaction).
	config.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createDailyTables(t, ctx, pool)

	for _, statement := range []string{
		"CREATE ROLE " + riverSchemaTestDomainRole +
			" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + riverSchemaTestDomainPassword + "'",
		"CREATE ROLE " + riverSchemaTestQueueRole +
			" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + riverSchemaTestQueuePassword + "'",
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, pool, riverstore.MigrationOptions{
		Schema:     "river",
		DomainRole: riverSchemaTestDomainRole,
		QueueRole:  riverSchemaTestQueueRole,
	}); err != nil {
		t.Fatal(err)
	}

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
	return pool, store, publisher
}
