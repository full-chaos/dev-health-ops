//go:build integration

package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/jackc/pgx/v5/pgxpool"
)

// newRemainingRedriveTestStack builds a real Postgres-backed store AND a
// real outbox-writing publisher (registry loaded from the checked-in job
// contracts, same as production) -- unlike the rest of this package's
// integration tests, which only ever pass nopPartitionPublisher and never
// touch worker_job_outbox at all. Proving `metrics remaining redrive`
// re-enqueues real, durable jobs needs the real write path.
func newRemainingRedriveTestStack(t *testing.T) (*pgxpool.Pool, *PostgresStore, *PostgresPublisher) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
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
	createRemainingTables(t, ctx, pool)
	createRemainingOutboxTable(t, ctx, pool)
	registry, err := jobruntime.Load(filepath.Join("..", "..", "..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewPostgresPublisher(pool, registry)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	return pool, store, publisher
}

// createRemainingOutboxTable adds worker_job_outbox alongside
// createRemainingTables's own tables -- column-for-column the same fixture
// shape internal/jobs/metrics/daily's own createDailyTables uses, since
// joboutbox.Producer's write path is family-agnostic. createRemainingTables
// itself now calls this (deadHandoffReasonSQL, embedded in
// findManualBackfillBlocker's own query, reads worker_job_outbox on every
// invocation, not just this file's redrive-focused tests), so the CREATE is
// idempotent: a caller that -- like this file's own stack builder -- still
// calls both explicitly hits IF NOT EXISTS rather than a duplicate-relation
// error.
func createRemainingOutboxTable(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	// The migrated schema, not hand-written tables (CHAOS-6769 ledger).
	pgschema.Apply(ctx, t, pool)
}

func capacityScopeJSON(historyDays int) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(
		`{"version":1,"all_teams":true,"history_days":%d,"simulations":10000}`, historyDays,
	))
}

// TestReleasePartitionTerminallyFinalizesRunOnlyWhenNoOtherPartitionIsOutstanding
// is the primary CHAOS gap-closing proof for the automatic half of this
// ticket: a deterministic ComputePartition precondition failure releases its
// partition to 'failed' via ReleasePartitionTerminally (the only caller,
// PartitionHandler.Work's Permanent/ErrInvalidState branch, since River
// discards that job outright with no further attempts). Before this,
// nothing ever moved remaining_metric_runs.status out of 'running' for that
// shape, even though the schema's own CHECK constraint has allowed 'failed'
// since migration 0058. Both directions are seeded: the last outstanding
// partition failing MUST terminalize the run; a failure while a sibling
// partition is still pending MUST NOT.
func TestReleasePartitionTerminallyFinalizesRunOnlyWhenNoOtherPartitionIsOutstanding(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := newRemainingRedriveTestStack(t)
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	assertRunStatus := func(t *testing.T, runID, want string) {
		t.Helper()
		var got string
		if err := pool.QueryRow(ctx,
			"SELECT status FROM remaining_metric_runs WHERE id = $1::uuid", runID).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("run status = %q, want %q", got, want)
		}
	}
	assertPartitionStatus := func(t *testing.T, partitionID, want string) {
		t.Helper()
		var got string
		if err := pool.QueryRow(ctx,
			"SELECT status FROM remaining_metric_partitions WHERE id = $1::uuid", partitionID).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("partition status = %q, want %q", got, want)
		}
	}

	t.Run("last outstanding partition terminalizes the run", func(t *testing.T) {
		run, err := store.StartRun(ctx, StartRunRequest{
			OrganizationID: "00000000-0000-4000-8000-000000005001",
			Family:         "capacity",
			Generation:     "terminal-single",
			ScopeKey:       "all-teams",
			GenerationSeed: int64Pointer(1),
			Scopes:         []json.RawMessage{capacityScopeJSON(90)},
		})
		if err != nil {
			t.Fatal(err)
		}
		partitionID := deterministicPartitionID(run.ID, 1)
		claim, err := store.ClaimPartition(ctx, partitionID)
		if err != nil || claim == nil {
			t.Fatalf("claim = %#v, %v", claim, err)
		}
		if err := store.ReleasePartitionTerminally(ctx, *claim); err != nil {
			t.Fatalf("ReleasePartitionTerminally = %v", err)
		}
		assertPartitionStatus(t, partitionID, "failed")
		assertRunStatus(t, run.ID, "failed")

		// A terminalized run's partition must never be reclaimable again --
		// ClaimPartition's own run.status IN ('pending','running') gate
		// already enforces this; asserted here so a regression in EITHER
		// this test's own setup or that gate is caught the same place.
		reclaim, err := store.ClaimPartition(ctx, partitionID)
		if err != nil || reclaim != nil {
			t.Fatalf("terminalized run's partition reclaimed = %#v, %v", reclaim, err)
		}
	})

	t.Run("a sibling partition still pending is not terminalized", func(t *testing.T) {
		run, err := store.StartRun(ctx, StartRunRequest{
			OrganizationID: "00000000-0000-4000-8000-000000005002",
			Family:         "capacity",
			Generation:     "terminal-sibling-pending",
			ScopeKey:       "all-teams",
			GenerationSeed: int64Pointer(2),
			Scopes:         []json.RawMessage{capacityScopeJSON(90), capacityScopeJSON(91)},
		})
		if err != nil {
			t.Fatal(err)
		}
		firstID := deterministicPartitionID(run.ID, 1)
		secondID := deterministicPartitionID(run.ID, 2)
		claim, err := store.ClaimPartition(ctx, firstID)
		if err != nil || claim == nil {
			t.Fatalf("claim = %#v, %v", claim, err)
		}
		if err := store.ReleasePartitionTerminally(ctx, *claim); err != nil {
			t.Fatalf("ReleasePartitionTerminally = %v", err)
		}
		assertPartitionStatus(t, firstID, "failed")
		// The second partition was never claimed -- still 'pending' -- so
		// the run must be left exactly where an ordinary ReleasePartition
		// would leave it: 'running', not force-terminalized underneath
		// still-live work.
		assertPartitionStatus(t, secondID, "pending")
		assertRunStatus(t, run.ID, "running")
	})

	t.Run("a sibling partition still running is not terminalized", func(t *testing.T) {
		run, err := store.StartRun(ctx, StartRunRequest{
			OrganizationID: "00000000-0000-4000-8000-000000005003",
			Family:         "capacity",
			Generation:     "terminal-sibling-running",
			ScopeKey:       "all-teams",
			GenerationSeed: int64Pointer(3),
			Scopes:         []json.RawMessage{capacityScopeJSON(90), capacityScopeJSON(91)},
		})
		if err != nil {
			t.Fatal(err)
		}
		firstID := deterministicPartitionID(run.ID, 1)
		secondID := deterministicPartitionID(run.ID, 2)
		firstClaim, err := store.ClaimPartition(ctx, firstID)
		if err != nil || firstClaim == nil {
			t.Fatalf("first claim = %#v, %v", firstClaim, err)
		}
		secondClaim, err := store.ClaimPartition(ctx, secondID)
		if err != nil || secondClaim == nil {
			t.Fatalf("second claim = %#v, %v", secondClaim, err)
		}
		if err := store.ReleasePartitionTerminally(ctx, *firstClaim); err != nil {
			t.Fatalf("ReleasePartitionTerminally = %v", err)
		}
		assertPartitionStatus(t, firstID, "failed")
		assertPartitionStatus(t, secondID, "running")
		assertRunStatus(t, run.ID, "running")
	})
}

// TestRedriveFailedPartitionsReenqueuesExactlyTheFailedPartitions is the
// `metrics remaining redrive` default-action proof: given a run stranded
// 'running' with a mix of succeeded and failed partitions (a real prod
// shape -- no redrive verb ever existed to recover it), a redrive must
// publish a fresh job for every currently-'failed' partition and touch
// nothing else -- not the succeeded partition, not the run's own status.
func TestRedriveFailedPartitionsReenqueuesExactlyTheFailedPartitions(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newRemainingRedriveTestStack(t)
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	const orgID = "00000000-0000-4000-8000-000000005101"
	run, err := store.StartRun(ctx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "capacity",
		Generation:     "redrive-mixed",
		ScopeKey:       "all-teams",
		GenerationSeed: int64Pointer(9),
		Scopes:         []json.RawMessage{capacityScopeJSON(90), capacityScopeJSON(91)},
	})
	if err != nil {
		t.Fatal(err)
	}
	succeededID := deterministicPartitionID(run.ID, 1)
	failedID := deterministicPartitionID(run.ID, 2)

	succeededClaim, err := store.ClaimPartition(ctx, succeededID)
	if err != nil || succeededClaim == nil {
		t.Fatalf("succeeded claim = %#v, %v", succeededClaim, err)
	}
	if err := store.CompletePartition(ctx, *succeededClaim, "rows=1"); err != nil {
		t.Fatal(err)
	}
	failedClaim, err := store.ClaimPartition(ctx, failedID)
	if err != nil || failedClaim == nil {
		t.Fatalf("failed claim = %#v, %v", failedClaim, err)
	}
	// The ordinary (non-terminal) release -- simulating an exhausted-retry
	// failure that is NOT the run's last outstanding partition at release
	// time, exactly the shape that needed a redrive verb before this run's
	// sibling ever succeeded.
	if err := store.ReleasePartition(ctx, *failedClaim); err != nil {
		t.Fatal(err)
	}

	candidates, err := store.StrandedRuns(ctx, orgID, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].RunID != run.ID || len(candidates[0].FailedPartitions) != 1 ||
		candidates[0].FailedPartitions[0].ID != failedID || candidates[0].SucceededCount != 1 {
		t.Fatalf("StrandedRuns = %#v", candidates)
	}

	outcome, err := store.Redrive(ctx, publisher, orgID, "", "", "redrive-nonce-1", false, "")
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Terminalize || outcome.RedrivenPartitions != 1 ||
		len(outcome.RedrivenRunIDs) != 1 || outcome.RedrivenRunIDs[0] != run.ID {
		t.Fatalf("Redrive outcome = %#v", outcome)
	}

	var outboxCount int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM worker_job_outbox WHERE dedupe_key = $1",
		"remaining:partition:redrive:"+failedID+":redrive-nonce-1",
	).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 1 {
		t.Fatalf("redrive outbox rows for the failed partition = %d, want 1", outboxCount)
	}
	var succeededRedriveCount int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM worker_job_outbox WHERE dedupe_key = $1",
		"remaining:partition:redrive:"+succeededID+":redrive-nonce-1",
	).Scan(&succeededRedriveCount); err != nil {
		t.Fatal(err)
	}
	if succeededRedriveCount != 0 {
		t.Fatalf("redrive published a job for the already-succeeded partition: %d rows", succeededRedriveCount)
	}

	// Redrive only publishes -- it must never itself touch run/partition
	// status.
	var runStatus string
	if err := pool.QueryRow(ctx, "SELECT status FROM remaining_metric_runs WHERE id = $1::uuid", run.ID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if runStatus != "running" {
		t.Fatalf("run status after redrive = %q, want unchanged 'running'", runStatus)
	}

	// A second redrive with a fresh nonce publishes a second, distinct job
	// for the still-failed partition -- proving the redrive-scoped dedupe
	// key (not the permanent "remaining:partition:"+id one PublishPartitionTx
	// uses) is what makes a repeat redrive possible at all.
	outcome2, err := store.Redrive(ctx, publisher, orgID, "", "", "redrive-nonce-2", false, "")
	if err != nil {
		t.Fatal(err)
	}
	if outcome2.RedrivenPartitions != 1 {
		t.Fatalf("second redrive outcome = %#v", outcome2)
	}
}

// TestRedriveTerminalizeIsRefusedUnlessExplicitlyRequestedWithAReason is the
// `metrics remaining redrive` terminalize-path guard: the default action
// must never terminalize a run (only ever re-enqueue), and even an explicit
// terminalize request is refused -- before touching anything -- without a
// stated reason, matching every other operator-authorized terminal action
// in this CLI.
func TestRedriveTerminalizeIsRefusedUnlessExplicitlyRequestedWithAReason(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newRemainingRedriveTestStack(t)
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	const orgID = "00000000-0000-4000-8000-000000005201"
	run, err := store.StartRun(ctx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "capacity",
		Generation:     "redrive-terminalize-guard",
		ScopeKey:       "all-teams",
		GenerationSeed: int64Pointer(4),
		Scopes:         []json.RawMessage{capacityScopeJSON(90), capacityScopeJSON(91)},
	})
	if err != nil {
		t.Fatal(err)
	}
	succeededID := deterministicPartitionID(run.ID, 1)
	failedID := deterministicPartitionID(run.ID, 2)
	succeededClaim, err := store.ClaimPartition(ctx, succeededID)
	if err != nil || succeededClaim == nil {
		t.Fatalf("succeeded claim = %#v, %v", succeededClaim, err)
	}
	if err := store.CompletePartition(ctx, *succeededClaim, "rows=1"); err != nil {
		t.Fatal(err)
	}
	failedClaim, err := store.ClaimPartition(ctx, failedID)
	if err != nil || failedClaim == nil {
		t.Fatalf("failed claim = %#v, %v", failedClaim, err)
	}
	if err := store.ReleasePartition(ctx, *failedClaim); err != nil {
		t.Fatal(err)
	}
	// Every partition has now settled to a terminal status (succeeded/
	// failed) with none pending/running -- exactly the shape a terminalize
	// would be safe to act on, so this run is a genuine test of the guard
	// itself, not of some OTHER reason terminalize declined to act.

	assertRunStatus := func(t *testing.T, want string) {
		t.Helper()
		var got string
		if err := pool.QueryRow(ctx, "SELECT status FROM remaining_metric_runs WHERE id = $1::uuid", run.ID).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("run status = %q, want %q", got, want)
		}
	}

	// Default (terminalize=false): must redrive, never terminalize.
	if _, err := store.Redrive(ctx, publisher, orgID, "", "", "guard-nonce-1", false, ""); err != nil {
		t.Fatal(err)
	}
	assertRunStatus(t, "running")

	// terminalize=true with no reason: refused before any write.
	if _, err := store.Redrive(ctx, publisher, orgID, "", "", "guard-nonce-2", true, ""); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("terminalize with no reason = %v, want ErrInvalidState", err)
	}
	assertRunStatus(t, "running")
	if _, err := store.Redrive(ctx, publisher, orgID, "", "", "guard-nonce-3", true, "   "); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("terminalize with a blank reason = %v, want ErrInvalidState", err)
	}
	assertRunStatus(t, "running")

	// terminalize=true WITH a reason: takes effect.
	outcome, err := store.Redrive(ctx, publisher, orgID, "", "", "guard-nonce-4", true, "operator reviewed: no automatic path will ever retry this run's failed partition")
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.Terminalize || len(outcome.TerminalizedRunIDs) != 1 || outcome.TerminalizedRunIDs[0] != run.ID {
		t.Fatalf("terminalize outcome = %#v", outcome)
	}
	assertRunStatus(t, "failed")
}

// TestStartRunCoverageChecksIgnoreFailedRuns guards the coverage side of
// this ticket: HasSucceededPartition/loadRunCoveringDay (both used by
// StartRunTx's cross-trigger dora dedup) already only ever match
// run.status = 'succeeded' -- a run this ticket's new terminalize logic
// moves to 'failed' must keep falling outside that coverage, exactly like
// it already does for 'pending'/'running'/'canceled'. A regression that
// widened either query to also treat 'failed' as coverage would silently
// tell every future automatic trigger for that org/day "nothing left to
// do", permanently freezing the day at whatever a doomed, never-succeeded
// attempt left behind.
func TestStartRunCoverageChecksIgnoreFailedRuns(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := newRemainingRedriveTestStack(t)
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	const orgID = "00000000-0000-4000-8000-000000005301"
	// A day 3 days in the past, so it is a CLOSED day: HasSucceededPartition
	// treats a 0-row completion as real coverage only once the day is
	// closed, and this test wants that axis fixed so status is the only
	// thing under test.
	day := now.AddDate(0, 0, -3).Format("2006-01-02")
	scope := json.RawMessage(`{"version":1,"day":"` + day + `","sink":"auto","interval":"daily","backfill_days":1}`)

	run, err := store.StartRun(ctx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "dora",
		Generation:     "coverage-guard-v1",
		ScopeKey:       day,
		Scopes:         []json.RawMessage{scope},
	})
	if err != nil {
		t.Fatal(err)
	}
	partitionID := deterministicPartitionID(run.ID, 1)
	claim, err := store.ClaimPartition(ctx, partitionID)
	if err != nil || claim == nil {
		t.Fatalf("claim = %#v, %v", claim, err)
	}
	// The run's only partition, released terminally: this run is now
	// 'failed', not 'succeeded'.
	if err := store.ReleasePartitionTerminally(ctx, *claim); err != nil {
		t.Fatal(err)
	}
	var runStatus string
	if err := pool.QueryRow(ctx, "SELECT status FROM remaining_metric_runs WHERE id = $1::uuid", run.ID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if runStatus != "failed" {
		t.Fatalf("seeded run status = %q, want 'failed' (test setup did not reach the shape it claims to)", runStatus)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	found, err := store.HasSucceededPartition(ctx, tx, orgID, "dora", day, scope)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("HasSucceededPartition reported coverage from a 'failed' run")
	}

	// The real-world consequence: a fresh trigger for the SAME org/day must
	// create a genuinely new run, not silently treat the failed one as
	// already covered.
	freshTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer freshTx.Rollback(ctx)
	freshRun, err := store.StartRunTx(ctx, freshTx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "dora",
		Generation:     "fixed-schedule:dora_daily_fanout:coverage-guard-retry",
		ScopeKey:       day,
		Scopes:         []json.RawMessage{scope},
	}, nopPartitionPublisher{})
	if err != nil {
		t.Fatal(err)
	}
	if err := freshTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if freshRun.ID == run.ID {
		t.Fatal("a fresh trigger for the same org/day reused the failed run's identity instead of creating a new one")
	}
	var freshPartitionStatus string
	if err := pool.QueryRow(ctx,
		"SELECT status FROM remaining_metric_partitions WHERE id = $1::uuid",
		deterministicPartitionID(freshRun.ID, 1),
	).Scan(&freshPartitionStatus); err != nil {
		t.Fatal(err)
	}
	if freshPartitionStatus != "pending" {
		t.Fatalf("fresh run's partition status = %q, want 'pending' (real new work, not a no-op)", freshPartitionStatus)
	}
}
