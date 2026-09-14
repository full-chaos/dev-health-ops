//go:build integration

package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// seedPendingCapacityRun starts (but never claims) one automatic-trigger
// capacity run/partition for orgID under generation, via nopPartitionPublisher
// -- so, unlike newRemainingRedriveTestStack's real publisher, NO
// worker_job_outbox row is ever written for it. That is itself one of the
// four eligibility cases (no outbox row at all, e.g. a fixed-schedule run
// that predates the Go outbox owning this handoff); callers that need one of
// the other three cases seed a worker_job_outbox row for the returned
// partition id afterward.
func seedPendingCapacityRun(
	t *testing.T, ctx context.Context, store *PostgresStore, pool *pgxpool.Pool,
	orgID, generation string, seed int64,
) (runID, partitionID string) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.StartRunTx(ctx, tx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "capacity",
		Generation:     generation,
		ScopeKey:       "all-teams",
		GenerationSeed: int64Pointer(seed),
		Scopes:         []json.RawMessage{capacityScopeJSON(90)},
	}, nopPartitionPublisher{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return run.ID, deterministicPartitionID(run.ID, 1)
}

// seedOutboxRow inserts a worker_job_outbox row keyed on partitionID's
// PublishPartitionTx dedupe key ("remaining:partition:"+partitionID), in
// whatever status/prerequisite shape the test needs. prerequisiteKey=""
// means no gate at all.
func seedOutboxRow(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	partitionID, status, prerequisiteKey string,
) {
	t.Helper()
	var prerequisite any
	if prerequisiteKey != "" {
		prerequisite = prerequisiteKey
	}
	_, err := pool.Exec(ctx, `
INSERT INTO worker_job_outbox (
    id, dedupe_key, job_kind, contract_version, args, payload_hash,
    queue, priority, max_attempts, scheduled_at, status, attempt_count,
    next_attempt_at, prerequisite_completion_key, created_at, updated_at
) VALUES (
    $1, $2, 'metrics.remaining.capacity', 1, '{}'::json,
    'sha256:0000000000000000000000000000000000000000000000000000000000000000',
    'default', 0, 5, now(), $3, 0, now(), $4, now(), now()
)`, uuid.New().String(), "remaining:partition:"+partitionID, status, prerequisite)
	if err != nil {
		t.Fatal(err)
	}
}

// seedCompletionFence writes a worker_job_completion_fences row directly --
// the durable success marker joboutbox.MarkCompletionTx mints, without
// needing a real predecessor run to succeed through.
func seedCompletionFence(t *testing.T, ctx context.Context, pool *pgxpool.Pool, completionKey string) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		"INSERT INTO worker_job_completion_fences (completion_key) VALUES ($1)", completionKey,
	); err != nil {
		t.Fatal(err)
	}
}

func assertRemainingRunAndPartitionStatus(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID, partitionID, wantRunStatus, wantPartitionStatus string,
) {
	t.Helper()
	var runStatus, partitionStatus string
	if err := pool.QueryRow(ctx, "SELECT status FROM remaining_metric_runs WHERE id = $1::uuid", runID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT status FROM remaining_metric_partitions WHERE id = $1::uuid", partitionID).Scan(&partitionStatus); err != nil {
		t.Fatal(err)
	}
	if runStatus != wantRunStatus || partitionStatus != wantPartitionStatus {
		t.Fatalf("run status = %q partition status = %q, want run=%q partition=%q",
			runStatus, partitionStatus, wantRunStatus, wantPartitionStatus)
	}
}

// TestUnstartableRunsEligibilityPredicate is the core proof for the
// dead-handoff sweep: the four cases deadHandoffReasonSQL must tell apart,
// on a real Postgres. A large batch of stuck 'pending' runs in production
// falls into exactly the first three shapes (no outbox row at all, an
// outbox row already 'dead', and an outbox row 'pending' behind a
// prerequisite fence that will never be written); the fourth (pending
// behind a fence that DOES exist) is the one shape that must stay
// untouched, because the relay can still deliver it.
func TestUnstartableRunsEligibilityPredicate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, store, _ := newRemainingRedriveTestStack(t)
	const orgID = "00000000-0000-4000-8000-000000005401"

	noRowRun, noRowPartition := seedPendingCapacityRun(t, ctx, store, pool, orgID, "unstartable-no-row", 1)
	// No worker_job_outbox row written at all for noRowPartition.

	deadRun, deadPartition := seedPendingCapacityRun(t, ctx, store, pool, orgID, "unstartable-dead", 2)
	seedOutboxRow(t, ctx, pool, deadPartition, "dead", "")

	missingFenceRun, missingFencePartition := seedPendingCapacityRun(t, ctx, store, pool, orgID, "unstartable-missing-fence", 3)
	const missingFenceKey = "work_graph_execution_request:00000000-0000-4000-8000-0000000f0001"
	seedOutboxRow(t, ctx, pool, missingFencePartition, "pending", missingFenceKey)
	// Deliberately no worker_job_completion_fences row for missingFenceKey.

	deliverableRun, deliverablePartition := seedPendingCapacityRun(t, ctx, store, pool, orgID, "unstartable-deliverable-fence-present", 4)
	const deliverableKey = "work_graph_execution_request:00000000-0000-4000-8000-0000000f0002"
	seedOutboxRow(t, ctx, pool, deliverablePartition, "pending", deliverableKey)
	seedCompletionFence(t, ctx, pool, deliverableKey)

	ungatedDeliverableRun, ungatedDeliverablePartition := seedPendingCapacityRun(t, ctx, store, pool, orgID, "unstartable-deliverable-ungated", 5)
	seedOutboxRow(t, ctx, pool, ungatedDeliverablePartition, "pending", "")

	found, err := store.UnstartableRuns(ctx, orgID, "", "")
	if err != nil {
		t.Fatal(err)
	}
	byRunID := map[string][]string{}
	for _, run := range found {
		byRunID[run.RunID] = run.Reasons
	}

	assertReasons := func(runID, wantReason string) {
		t.Helper()
		reasons, ok := byRunID[runID]
		if !ok {
			t.Fatalf("run %s missing from UnstartableRuns, want reason %q", runID, wantReason)
		}
		if len(reasons) != 1 || reasons[0] != wantReason {
			t.Fatalf("run %s reasons = %v, want [%q]", runID, reasons, wantReason)
		}
	}
	assertReasons(noRowRun, "no_outbox_row")
	assertReasons(deadRun, "dead_outbox")
	assertReasons(missingFenceRun, "missing_fence")

	if _, ok := byRunID[deliverableRun]; ok {
		t.Fatalf("run %s (pending behind a fence that EXISTS) must not be eligible, found reasons %v", deliverableRun, byRunID[deliverableRun])
	}
	if _, ok := byRunID[ungatedDeliverableRun]; ok {
		t.Fatalf("run %s (ungated pending outbox row) must not be eligible, found reasons %v", ungatedDeliverableRun, byRunID[ungatedDeliverableRun])
	}
	if len(found) != 3 {
		t.Fatalf("UnstartableRuns = %#v, want exactly the 3 dead-handoff runs", found)
	}

	// Sanity: every seeded partition is still genuinely 'pending' going into
	// the write-path tests below.
	for _, id := range []string{noRowPartition, deadPartition, missingFencePartition, deliverablePartition, ungatedDeliverablePartition} {
		var status string
		if err := pool.QueryRow(ctx, "SELECT status FROM remaining_metric_partitions WHERE id = $1::uuid", id).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "pending" {
			t.Fatalf("partition %s status = %q, want pending (fixture bug)", id, status)
		}
	}
}

// TestRedriveTerminalizeUnstartableRunsRequiresReasonAndWritesNothingUntilThen
// covers: the read-only listing (UnstartableRuns, what a --dry-run prints)
// never mutates anything; terminalize is refused without --review-evidence,
// before any write; and terminalize WITH a reason moves both the run and its
// partition to 'failed' inside one transaction, but only the dead-handoff
// run -- a sibling with a deliverable handoff is left untouched.
func TestRedriveTerminalizeUnstartableRunsRequiresReasonAndWritesNothingUntilThen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, store, publisher := newRemainingRedriveTestStack(t)
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	const orgID = "00000000-0000-4000-8000-000000005402"

	deadRun, deadPartition := seedPendingCapacityRun(t, ctx, store, pool, orgID, "unstartable-terminalize-dead", 1)
	seedOutboxRow(t, ctx, pool, deadPartition, "dead", "")

	liveRun, livePartition := seedPendingCapacityRun(t, ctx, store, pool, orgID, "unstartable-terminalize-live", 2)
	seedOutboxRow(t, ctx, pool, livePartition, "pending", "")

	// Dry-run equivalent: the read path alone must never write anything.
	if _, err := store.UnstartableRuns(ctx, orgID, "", ""); err != nil {
		t.Fatal(err)
	}
	assertRemainingRunAndPartitionStatus(t, ctx, pool, deadRun, deadPartition, "pending", "pending")
	assertRemainingRunAndPartitionStatus(t, ctx, pool, liveRun, livePartition, "pending", "pending")

	// terminalize=true with no reason: refused before any write.
	if _, err := store.Redrive(ctx, publisher, orgID, "", "", "nonce-1", true, ""); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("terminalize with no reason = %v, want ErrInvalidState", err)
	}
	assertRemainingRunAndPartitionStatus(t, ctx, pool, deadRun, deadPartition, "pending", "pending")

	// terminalize=true with a reason: the dead-handoff run goes 'failed',
	// partition included; the live-handoff sibling is untouched.
	outcome, err := store.Redrive(ctx, publisher, orgID, "", "", "nonce-2", true,
		"operator reviewed: no fence will ever be written for this run's prerequisite")
	if err != nil {
		t.Fatal(err)
	}
	if len(outcome.TerminalizedUnstartableRunIDs) != 1 || outcome.TerminalizedUnstartableRunIDs[0] != deadRun {
		t.Fatalf("TerminalizedUnstartableRunIDs = %#v, want exactly [%s]", outcome.TerminalizedUnstartableRunIDs, deadRun)
	}
	assertRemainingRunAndPartitionStatus(t, ctx, pool, deadRun, deadPartition, "failed", "failed")
	assertRemainingRunAndPartitionStatus(t, ctx, pool, liveRun, livePartition, "pending", "pending")

	// A terminalized run's partition must never be reclaimable again.
	reclaim, err := store.ClaimPartition(ctx, deadPartition)
	if err != nil || reclaim != nil {
		t.Fatalf("terminalized unstartable run's partition reclaimed = %#v, %v", reclaim, err)
	}

	// A second terminalize pass finds nothing left to do for this org.
	outcome2, err := store.Redrive(ctx, publisher, orgID, "", "", "nonce-3", true, "operator reviewed: repeat pass")
	if err != nil {
		t.Fatal(err)
	}
	if len(outcome2.TerminalizedUnstartableRunIDs) != 0 {
		t.Fatalf("second terminalize pass = %#v, want nothing left", outcome2.TerminalizedUnstartableRunIDs)
	}
}

// TestFindManualBackfillBlockerIgnoresADeadHandoffPendingRunButStillCountsALiveOne
// is the findManualBackfillBlocker fix: a 'pending' run whose
// handoff is provably dead under the SAME deadHandoffReasonSQL predicate
// UnstartableRuns uses must never again read as blockReasonInProgress (it
// will never be claimed, so it can never collide with a manual backfill);
// a 'pending' run whose handoff is still genuinely deliverable must keep
// blocking, exactly as before.
func TestFindManualBackfillBlockerIgnoresADeadHandoffPendingRunButStillCountsALiveOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, store, _ := newRemainingRedriveTestStack(t)
	const orgID = "00000000-0000-4000-8000-000000005403"
	const day = "2026-08-25"

	t.Run("dead handoff is not a blocker", func(t *testing.T) {
		zombieRunID := seedPendingDoraPartition(t, ctx, store, pool, orgID, day, "post-sync:zombie")
		// seedPendingDoraPartition uses nopPartitionPublisher{} -- no
		// worker_job_outbox row was ever written, so this run's single
		// partition classifies as 'no_outbox_row': dead.

		outcome, err := store.StartManualBackfillRun(ctx, "dora", orgID, day, "manual-backfill:dead-handoff-check", nopPartitionPublisher{})
		if err != nil {
			t.Fatalf("expected the manual backfill to proceed past the zombie pending run %s, got err=%v", zombieRunID, err)
		}
		if outcome.RunID == zombieRunID {
			t.Fatalf("expected a fresh manual run, got the zombie run's own id back")
		}
	})

	t.Run("a live pending run still blocks", func(t *testing.T) {
		const liveDay = "2026-08-26"
		liveRunID := seedPendingDoraPartition(t, ctx, store, pool, orgID, liveDay, "post-sync:in-flight")
		partitionID := deterministicPartitionID(liveRunID, 1)
		// Unlike the zombie case, this run's handoff is genuinely
		// deliverable: an ungated 'pending' outbox row exists for it.
		seedOutboxRow(t, ctx, pool, partitionID, "pending", "")

		outcome, err := store.StartManualBackfillRun(ctx, "dora", orgID, liveDay, "manual-backfill:live-handoff-check", nopPartitionPublisher{})
		if !errors.Is(err, ErrDayInProgress) {
			t.Fatalf("expected ErrDayInProgress for a day with a genuinely live pending run, got err=%v outcome=%+v", err, outcome)
		}
		if outcome.RunID != liveRunID {
			t.Fatalf("expected the refusal to report the live run %s, got %q", liveRunID, outcome.RunID)
		}
	})
}
