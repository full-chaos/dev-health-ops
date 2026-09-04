//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// blockedFixture builds one run with the partition statuses named, so each
// case below differs ONLY in the partition state the predicate reads.
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
	for ordinal, status := range fixture.partitionState {
		var reason any
		if status == "failed_permanent" {
			reason = "ambiguous_refused"
		}
		if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_partitions (id,run_id,ordinal,repo_ids,status,attempt_count,created_at,updated_at,failure_reason) VALUES ($1,$2,$3,'[]'::jsonb,$4,0,$5,$5,$6)`,
			uuid.NewString(), fixture.runID, ordinal, status, now, reason); err != nil {
			t.Fatalf("seeding partition %d status=%s: %v", ordinal, status, err)
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
			// ClaimPartition reclaims a 'running' partition whose lease has
			// expired, so progress is still possible. Treating "running" as
			// stuck would mark healthy in-flight runs.
			name:        "failed_permanent alongside a running is not blocked",
			partitions:  []string{"failed_permanent", "running"},
			wantBlocked: false,
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
