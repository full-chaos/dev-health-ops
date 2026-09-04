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

// blockedFixture builds one run with the partition statuses named, so each
// case below differs ONLY in the partition state the predicate reads.
// The two running shapes, kept as constants so a test cannot silently mean the
// wrong one. An EXPIRED lease is the stranded shape codex review round 2 found:
// the final River attempt died after ClaimPartition succeeded, so the row is
// 'running' forever with nothing left to reclaim it.
const (
	runningLiveLease    = "running_live_lease"
	runningExpiredLease = "running_expired_lease"
)

type blockedFixture struct {
	runID          string
	orgID          string
	partitionState []string
}

func seedBlockedRun(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, fixture blockedFixture, now time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_runs (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at) VALUES ($1,$2,'2026-09-01','daily-v1','running','pending',$3,$3)`,
		fixture.runID, fixture.orgID, now); err != nil {
		t.Fatal(err)
	}
	for ordinal, state := range fixture.partitionState {
		// 'running' is not one state but two, and the difference decides
		// whether the run can still advance. The fixture now carries
		// production's ck_daily_metrics_partition_lease, so a running row MUST
		// carry a claim_token and a lease -- writing "running" with neither,
		// which this helper used to do, describes a row production forbids.
		status, reason, lease := state, any(nil), any(nil)
		switch state {
		case "failed_permanent":
			reason = "ambiguous_refused"
		case runningLiveLease:
			status, lease = "running", now.Add(10*time.Minute)
		case runningExpiredLease:
			status, lease = "running", now.Add(-time.Minute)
		case "running":
			t.Fatalf("partition %d: use %q or %q, never a bare \"running\" -- "+
				"the lease is the whole question", ordinal, runningLiveLease, runningExpiredLease)
		}
		var token any
		if lease != nil {
			token = uuid.NewString()
		}
		if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_partitions (id,run_id,ordinal,repo_ids,status,attempt_count,created_at,updated_at,failure_reason,claim_token,lease_expires_at) VALUES ($1,$2,$3,'[]'::jsonb,$4,0,$5,$5,$6,$7::uuid,$8)`,
			uuid.NewString(), fixture.runID, ordinal, status, now, reason, token, lease); err != nil {
			t.Fatalf("seeding partition %d state=%s: %v", ordinal, state, err)
		}
	}
}

func readMarker(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string) (bool, string) {
	t.Helper()
	var blockedAt *time.Time
	var reason *string
	if err := pool.QueryRow(ctx,
		`SELECT blocked_at, blocked_reason FROM daily_metrics_runs WHERE id = $1::uuid`, runID,
	).Scan(&blockedAt, &reason); err != nil {
		t.Fatal(err)
	}
	if blockedAt == nil {
		return false, ""
	}
	return true, *reason
}

// The predicate itself, in both directions, against a real Postgres. A unit
// test cannot cover this: the whole decision lives in SQL, and the paired
// CHECK constraint is enforced by the database, not by Go.
func TestReconcileBlockedRunsMarksOnlyRunsThatCanNeverFinish(t *testing.T) {
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
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)

	for _, testCase := range []struct {
		name        string
		partitions  []string
		wantBlocked bool
		wantReason  string
	}{
		{
			// The CHAOS-4970 shape: real output for one repo, one repo
			// permanently refused. The fence can never be written.
			name:        "mixed succeeded and failed_permanent is blocked",
			partitions:  []string{"succeeded", "failed_permanent"},
			wantBlocked: true,
			wantReason:  BlockedReasonPartialPartitionsPermanent,
		},
		{
			name:        "all failed_permanent is blocked",
			partitions:  []string{"failed_permanent", "failed_permanent"},
			wantBlocked: true,
			wantReason:  BlockedReasonAllPartitionsPermanent,
		},
		{
			// A 'failed' partition is re-dispatchable, so this run can still
			// finish on its own and must NOT be marked -- marking it would
			// raise a false alarm on ordinary transient failure.
			name:        "failed_permanent alongside a dispatchable failed is not blocked",
			partitions:  []string{"failed_permanent", "failed"},
			wantBlocked: false,
		},
		{
			name:        "failed_permanent alongside a pending is not blocked",
			partitions:  []string{"failed_permanent", "pending"},
			wantBlocked: false,
		},
		{
			// A LIVE lease means a worker is holding the partition right now,
			// so the run is healthy in-flight work and must not be marked.
			name:        "failed_permanent alongside a live-lease running is not blocked",
			partitions:  []string{"failed_permanent", runningLiveLease},
			wantBlocked: false,
		},
		{
			// codex review round 2 (P2). This case previously read "running is
			// not blocked", justified by "ClaimPartition reclaims an expired
			// lease, so progress is still possible". The first half is true --
			// classifyLease's leaseReclaimable branch does exactly that -- but
			// the conclusion does not follow: ClaimPartition has to be CALLED,
			// and only a metrics.daily_partition job calls it. DispatchablePartitions
			// returns only 'pending'/'failed' (postgres.go), so once the final
			// River attempt dies after claiming, nothing automatic ever
			// re-publishes a job for that row and nothing ever reclaims it.
			//
			// redrive.go:188 already treats this exact shape as redrivable.
			// Leaving the marker disagreeing with the redrive would mean two
			// definitions of "stuck" in one package -- the drift this design
			// avoids everywhere else.
			name:        "failed_permanent alongside an expired-lease running IS blocked",
			partitions:  []string{"failed_permanent", runningExpiredLease},
			wantBlocked: true,
			// ALL, not PARTIAL, and the distinction is about OUTPUT rather
			// than a literal partition census: nothing here succeeded, so
			// there is no computed output a redrive could needlessly
			// recompute. That is the question the reason answers for an
			// operator. The name reads a little loosely against this fixture
			// -- one partition is running, not failed_permanent -- but the
			// signal it carries ("nothing to preserve, redrive freely") is
			// exactly right, and it is the signal the redrive decision needs.
			// The succeeded-alongside case is covered by
			// TestAnExpiredLeaseRunningPartitionLeavesTheRunStrandedAndMarked,
			// which asserts PARTIAL for the same shape plus a succeeded
			// partition.
			wantReason: BlockedReasonAllPartitionsPermanent,
		},
		{
			name:        "all succeeded is not blocked",
			partitions:  []string{"succeeded", "succeeded"},
			wantBlocked: false,
		},
		{
			// No failed_permanent at all: an ordinary run mid-flight.
			name:        "pending only is not blocked",
			partitions:  []string{"pending"},
			wantBlocked: false,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runID := uuid.NewString()
			seedBlockedRun(t, ctx, pool, blockedFixture{
				runID: runID, orgID: orgID, partitionState: testCase.partitions,
			}, now)
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
				t.Fatalf("blocked = %t, want %t (partitions %v)", blocked, testCase.wantBlocked, testCase.partitions)
			}
			if blocked && reason != testCase.wantReason {
				t.Fatalf("reason = %q, want %q", reason, testCase.wantReason)
			}
		})
	}
}

// Reversibility, the constraint that makes the marker safe to add: a redrive
// resets failed_permanent partitions to 'failed', which makes the predicate
// false, and the NEXT pass must clear the marker on its own. Nothing has to
// remember to undo it -- if this fails, introducing the marker would make a
// redrive a dead end, which is strictly worse than the bug it reports.
func TestReconcileBlockedRunsClearsTheMarkerWhenARedriveMakesTheRunLiveAgain(t *testing.T) {
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

	orgID, runID := uuid.NewString(), uuid.NewString()
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	seedBlockedRun(t, ctx, pool, blockedFixture{
		runID: runID, orgID: orgID,
		partitionState: []string{"succeeded", "failed_permanent"},
	}, now)
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }

	first, err := store.ReconcileBlockedRuns(ctx, orgID)
	if err != nil {
		t.Fatal(err)
	}
	if first.Marked != 1 || first.Cleared != 0 || first.Blocked != 1 {
		t.Fatalf("first pass = %+v, want marked 1 / cleared 0 / blocked 1", first)
	}
	if blocked, _ := readMarker(t, ctx, pool, runID); !blocked {
		t.Fatal("run was not marked blocked")
	}

	// A second pass with NOTHING changed must write nothing -- but must still
	// report the run as blocked. Deriving the total from the deltas instead
	// of counting it would report 0 here, the exact silent zero an alert
	// would read as "no wedged runs".
	steady, err := store.ReconcileBlockedRuns(ctx, orgID)
	if err != nil {
		t.Fatal(err)
	}
	if steady.Marked != 0 || steady.Cleared != 0 {
		t.Fatalf("steady-state pass = %+v, want no writes", steady)
	}
	if steady.Blocked != 1 {
		t.Fatalf("steady-state blocked = %d, want 1 -- a steady state must not read as zero", steady.Blocked)
	}

	// Exactly what RedriveStrandedPartitions step 1 does.
	if _, err := pool.Exec(ctx,
		`UPDATE daily_metrics_partitions SET status = 'failed', failure_reason = NULL WHERE run_id = $1::uuid AND status = 'failed_permanent'`,
		runID); err != nil {
		t.Fatal(err)
	}

	after, err := store.ReconcileBlockedRuns(ctx, orgID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Marked != 0 || after.Cleared != 1 || after.Blocked != 0 {
		t.Fatalf("post-redrive pass = %+v, want marked 0 / cleared 1 / blocked 0", after)
	}
	blocked, reason := readMarker(t, ctx, pool, runID)
	if blocked {
		t.Fatalf("marker survived the redrive (reason %q) -- the redrive is a dead end", reason)
	}
}

// A pass is per-organization, so it must never touch another tenant's runs.
func TestReconcileBlockedRunsIsScopedToItsOwnOrganization(t *testing.T) {
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

	mine, theirs := uuid.NewString(), uuid.NewString()
	myRun, theirRun := uuid.NewString(), uuid.NewString()
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	wedged := []string{"succeeded", "failed_permanent"}
	seedBlockedRun(t, ctx, pool, blockedFixture{runID: myRun, orgID: mine, partitionState: wedged}, now)
	seedBlockedRun(t, ctx, pool, blockedFixture{runID: theirRun, orgID: theirs, partitionState: wedged}, now)

	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	outcome, err := store.ReconcileBlockedRuns(ctx, mine)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Marked != 1 || outcome.Blocked != 1 {
		t.Fatalf("outcome = %+v, want exactly this organization's one run", outcome)
	}
	if blocked, _ := readMarker(t, ctx, pool, myRun); !blocked {
		t.Fatal("own run was not marked")
	}
	if blocked, _ := readMarker(t, ctx, pool, theirRun); blocked {
		t.Fatal("another organization's run was marked by this organization's pass")
	}
}

// The database, not Go, is the last line of defence on the marker's pairing.
func TestBlockedMarkerColumnsAreRejectedUnpaired(t *testing.T) {
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

	orgID, runID := uuid.NewString(), uuid.NewString()
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	seedBlockedRun(t, ctx, pool, blockedFixture{
		runID: runID, orgID: orgID, partitionState: []string{"succeeded"},
	}, now)

	if _, err := pool.Exec(ctx,
		`UPDATE daily_metrics_runs SET blocked_at = $2 WHERE id = $1::uuid`, runID, now); err == nil {
		t.Fatal("a blocked_at with no reason was accepted -- the paired CHECK is not enforcing")
	}
	if _, err := pool.Exec(ctx,
		`UPDATE daily_metrics_runs SET blocked_reason = 'x' WHERE id = $1::uuid`, runID); err == nil {
		t.Fatal("a blocked_reason with no timestamp was accepted -- the paired CHECK is not enforcing")
	}
	// Control: the paired form IS accepted, so the two refusals above are the
	// constraint discriminating, not the UPDATE failing for some other reason.
	if _, err := pool.Exec(ctx,
		`UPDATE daily_metrics_runs SET blocked_at = $2, blocked_reason = $3 WHERE id = $1::uuid`,
		runID, now, BlockedReasonAllPartitionsPermanent); err != nil {
		t.Fatalf("the paired form was rejected too: %v", err)
	}
}

// The readback is the operator's half of this change: a marker nobody can
// interpret is the same silent freeze in a new place.
func TestBlockedRunsReadbackExplainsWhyTheRunIsStuck(t *testing.T) {
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

	orgID, runID := uuid.NewString(), uuid.NewString()
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	seedBlockedRun(t, ctx, pool, blockedFixture{
		runID: runID, orgID: orgID,
		partitionState: []string{"succeeded", "succeeded", "failed_permanent"},
	}, now)
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	if _, err := store.ReconcileBlockedRuns(ctx, orgID); err != nil {
		t.Fatal(err)
	}

	rows, err := store.BlockedRuns(ctx, orgID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("readback returned %d rows, want 1", len(rows))
	}
	row := rows[0]
	if row.RunID != runID {
		t.Fatalf("run id = %s, want %s", row.RunID, runID)
	}
	if row.Reason != BlockedReasonPartialPartitionsPermanent {
		t.Fatalf("reason = %q, want %q", row.Reason, BlockedReasonPartialPartitionsPermanent)
	}
	// The operator needs to know WHAT refused, not just that something did.
	if len(row.FailureReasons) != 1 || row.FailureReasons[0] != "ambiguous_refused" {
		t.Fatalf("failure reasons = %v, want [ambiguous_refused]", row.FailureReasons)
	}
	// And how much real output a redrive would recompute -- the file_hotspots
	// double-write hazard makes a needless recompute genuinely costly.
	if row.PermanentPartitions != 1 || row.SucceededPartitions != 2 {
		t.Fatalf("counts = %d permanent / %d succeeded, want 1 / 2",
			row.PermanentPartitions, row.SucceededPartitions)
	}

	// An unblocked organization's readback is empty, not an error.
	empty, err := store.BlockedRuns(ctx, uuid.NewString(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("readback for an organization with no blocked runs returned %d rows", len(empty))
	}
}

// waitForALockedStatement blocks until some backend in this database is
// waiting on a heavyweight lock, which is how this file knows the redrive's
// step-1 UPDATE has actually STARTED and taken its snapshot rather than merely
// having been called. Sleeping a fixed interval instead would make the test
// prove nothing on a slow host: the interleaving it depends on would not have
// happened yet and it would pass for the wrong reason.
func waitForALockedStatement(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database()
			  AND state = 'active'
			  AND wait_event_type = 'Lock'`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no backend ever blocked on a lock; the redrive never reached its step-1 UPDATE")
}

// codex review round 1, P2 -- the finding this test exists to hold closed.
//
// The marker clear used to be a SECOND statement scoped by org+day rather than
// by the rows step 1 actually reset. Under Read Committed the two statements
// take separate snapshots, so a run that becomes blocked BETWEEN them loses its
// marker without its partition being reset -- and step 2 excludes
// failed_permanent, so nothing is published for it either. The run stays wedged
// AND reports itself healthy, which is worse than never having cleared it.
//
// The interleaving is forced, not simulated: a second connection holds a row
// lock on the redrivable run's partition, so the redrive's step-1 statement
// takes its snapshot and then WAITS. While it waits, a concurrent fan-out
// reconcile marks a brand-new blocked run in the same org and day range. That
// run is invisible to the already-started step 1, but a later second statement
// would see it -- which is precisely the gap.
//
// This drives the REAL RedriveStrandedPartitions. The reversibility test above
// cannot catch this class because it flips a partition by hand and never enters
// the redrive at all.
func TestRedriveClearsOnlyTheRunsItActuallyReset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	orgID := uuid.NewString()
	redrivable := uuid.NewString()
	seedBlockedRun(t, ctx, pool, blockedFixture{
		runID: redrivable, orgID: orgID,
		partitionState: []string{"succeeded", "failed_permanent"},
	}, now)
	if _, err := store.ReconcileBlockedRuns(ctx, orgID); err != nil {
		t.Fatal(err)
	}
	if blocked, _ := readMarker(t, ctx, pool, redrivable); !blocked {
		t.Fatal("precondition: the redrivable run should have been marked blocked")
	}

	// Hold a row lock on the partition step 1 wants, on a connection the
	// redrive does not own.
	blocker, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Release()
	holdTx, err := blocker.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holdTx.Exec(ctx,
		`SELECT 1 FROM daily_metrics_partitions WHERE run_id = $1::uuid FOR UPDATE`, redrivable,
	); err != nil {
		t.Fatal(err)
	}

	targetDay := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	type redriveResult struct {
		outcome RedriveOutcome
		err     error
	}
	done := make(chan redriveResult, 1)
	go func() {
		outcome, err := store.RedriveStrandedPartitions(
			ctx, publisher, orgID, targetDay, targetDay, "redrive-blocked-race-nonce",
		)
		done <- redriveResult{outcome: outcome, err: err}
	}()

	// Step 1 has now taken its snapshot and is waiting on the lock above.
	waitForALockedStatement(t, ctx, pool)

	// The concurrent fan-out: a DIFFERENT run wedges and is marked, after the
	// redrive's step 1 started and therefore invisible to it.
	concurrentlyBlocked := uuid.NewString()
	seedBlockedRun(t, ctx, pool, blockedFixture{
		runID: concurrentlyBlocked, orgID: orgID,
		partitionState: []string{"succeeded", "failed_permanent"},
	}, now)
	if _, err := store.ReconcileBlockedRuns(ctx, orgID); err != nil {
		t.Fatal(err)
	}
	if blocked, _ := readMarker(t, ctx, pool, concurrentlyBlocked); !blocked {
		t.Fatal("precondition: the concurrently-wedged run should have been marked blocked")
	}

	if err := holdTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	result := <-done
	if result.err != nil {
		t.Fatalf("RedriveStrandedPartitions: %v", result.err)
	}
	if result.outcome.PermanentReset != 1 {
		t.Fatalf("PermanentReset = %d, want 1 -- only the run visible to step 1's snapshot",
			result.outcome.PermanentReset)
	}

	// The run the redrive DID reset loses its marker: the latency optimisation
	// still works, so this test cannot pass by the clear having been deleted.
	if blocked, _ := readMarker(t, ctx, pool, redrivable); blocked {
		t.Fatal("the redriven run kept its marker; the clear no longer fires at all")
	}
	// The run it did NOT reset keeps its marker. Losing it here is the P2:
	// still wedged, still holding a failed_permanent partition, nothing
	// published for it, and now reporting itself healthy.
	if blocked, reason := readMarker(t, ctx, pool, concurrentlyBlocked); !blocked {
		t.Fatalf("a run the redrive never reset lost its marker (reason now %q) -- "+
			"it is still wedged and now reports healthy", reason)
	}
	// It is also genuinely still wedged, not merely still marked: its
	// failed_permanent partition was never reset, so the marker is telling the
	// truth and clearing it would have been a lie rather than a race that
	// happened to be harmless.
	var stillPermanent int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM daily_metrics_partitions WHERE run_id = $1::uuid AND status = 'failed_permanent'`,
		concurrentlyBlocked,
	).Scan(&stillPermanent); err != nil {
		t.Fatal(err)
	}
	if stillPermanent != 1 {
		t.Fatalf("failed_permanent partitions on the untouched run = %d, want 1", stillPermanent)
	}
}

// codex review round 2, P2 -- the finding this test exists to hold closed.
//
// A run holding [succeeded, failed_permanent, running-with-EXPIRED-lease] is
// permanently stranded when the final River attempt died AFTER ClaimPartition
// succeeded and its attempt budget is spent. The old predicate read any
// 'running' partition as "something can still happen" and left the run
// unmarked -- the marker staying silent on exactly the kind of freeze it
// exists to expose.
//
// The two assertions have to be made TOGETHER. Asserting only that the run is
// marked would not show the run is genuinely stuck, and asserting only that
// nothing is dispatchable would not show the marker noticed. Together they say:
// nothing automatic can advance this run, AND the marker says so.
func TestAnExpiredLeaseRunningPartitionLeavesTheRunStrandedAndMarked(t *testing.T) {
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
	runID := uuid.NewString()
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	// Seeded through the shared helper, so the row satisfies production's
	// ck_daily_metrics_partition_lease -- claim_token and lease_expires_at both
	// present, the lease simply in the past. A hand-written row without a token
	// would describe a state production cannot hold.
	seedBlockedRun(t, ctx, pool, blockedFixture{
		runID: runID, orgID: orgID,
		partitionState: []string{"succeeded", "failed_permanent", runningExpiredLease},
	}, now)

	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }

	// Leg 1: nothing automatic can advance this run. DispatchablePartitions is
	// what the fan-out publishes from, and it returns only 'pending'/'failed',
	// so the expired-lease row is invisible to it. ClaimPartition WOULD reclaim
	// that row -- but only a metrics.daily_partition job calls ClaimPartition,
	// and nothing here will ever create one.
	dispatchable, err := store.DispatchablePartitions(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(dispatchable) != 0 {
		t.Fatalf("DispatchablePartitions = %#v, want empty -- if this is non-empty "+
			"the run is not stranded and the premise of this test is wrong", dispatchable)
	}

	// Leg 2: the marker must say so.
	outcome, err := store.ReconcileBlockedRuns(ctx, orgID)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Marked != 1 {
		t.Fatalf("Marked = %d, want 1 -- a run nothing can advance was left unmarked", outcome.Marked)
	}
	blocked, reason := readMarker(t, ctx, pool, runID)
	if !blocked {
		t.Fatal("the stranded run carries no blocked marker")
	}
	if reason != BlockedReasonPartialPartitionsPermanent {
		t.Fatalf("reason = %q, want %q -- some partitions succeeded, so a redrive "+
			"decision needs to know partial output exists",
			reason, BlockedReasonPartialPartitionsPermanent)
	}

	// Control, and the reason this cannot be "mark anything with a running
	// partition": the SAME shape with a LIVE lease is healthy in-flight work
	// and must stay unmarked. Without this the fix could be a blanket
	// "running means stuck", which would mark every run being worked on.
	liveRunID := uuid.NewString()
	seedBlockedRun(t, ctx, pool, blockedFixture{
		runID: liveRunID, orgID: orgID,
		partitionState: []string{"succeeded", "failed_permanent", runningLiveLease},
	}, now)
	if _, err := store.ReconcileBlockedRuns(ctx, orgID); err != nil {
		t.Fatal(err)
	}
	if blocked, _ := readMarker(t, ctx, pool, liveRunID); blocked {
		t.Fatal("a run whose partition is held under a LIVE lease was marked blocked -- " +
			"that is healthy in-flight work")
	}
}
