//go:build integration

package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// seedSucceededDoraPartition starts and completes one automatic-trigger dora
// run/partition for (orgID, day) with the given rows_written, in exactly the
// format compatibilityCompletionResult (handler.go) and
// DORAExecutor.ComputePartition write it -- the real shape, not a
// hand-authored evidence string. Returns the seeded run's id.
func seedSucceededDoraPartition(
	t *testing.T, ctx context.Context, store *PostgresStore, pool *pgxpool.Pool,
	orgID, day string, rowsWritten int, generation string,
) string {
	t.Helper()
	return seedSucceededDoraPartitionWithBackfillDays(t, ctx, store, pool, orgID, day, 1, rowsWritten, generation)
}

// seedSucceededDoraPartitionWithBackfillDays is seedSucceededDoraPartition
// generalized to an anchor day whose window covers backfillDays days ending
// on it (DORAExecutor.ComputePartition's dayRange semantics) -- used to seed
// a wide post-sync catch-up run that covers several INTERIOR days from one
// partition anchored on the latest of them.
func seedSucceededDoraPartitionWithBackfillDays(
	t *testing.T, ctx context.Context, store *PostgresStore, pool *pgxpool.Pool,
	orgID, anchorDay string, backfillDays, rowsWritten int, generation string,
) string {
	t.Helper()
	scope := json.RawMessage(`{"version":1,"day":"` + anchorDay + `","sink":"auto","interval":"daily","backfill_days":` + strconv.Itoa(backfillDays) + `}`)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.StartRunTx(ctx, tx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "dora",
		Generation:     generation,
		ScopeKey:       generation,
		Scopes:         []json.RawMessage{scope},
	}, nopPartitionPublisher{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	partitionID := deterministicPartitionID(run.ID, 1)
	claim, err := store.ClaimPartition(ctx, partitionID)
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil {
		t.Fatalf("seed run %s partition was not claimable", run.ID)
	}
	evidence := "compatibility_execution:" + partitionID + ":rows_written=" + strconv.Itoa(rowsWritten)
	if err := store.CompletePartition(ctx, *claim, evidence); err != nil {
		t.Fatal(err)
	}
	return run.ID
}

func TestStartManualBackfillRunRecomputesAZeroRowClosedDay(t *testing.T) {
	// Red-on-baseline for CHAOS-4254's whole reason to exist: the automatic
	// dora path (StartRunTx's family=="dora" block, CHAOS-4384) treats ANY
	// succeeded partition for a CLOSED day as terminal coverage, 0 rows or
	// not -- that is correct for the automatic triggers, but it means a day
	// frozen at 0 rows before the CHAOS-4384 fix landed stays frozen
	// forever, even after real source data lands, unless something bypasses
	// that same-day dedup. This is the manual backfill's entire job.
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
	createRemainingTables(t, ctx, pool)

	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	const orgID = "00000000-0000-4000-8000-000000004254"
	closedDay := time.Now().UTC().AddDate(0, 0, -3).Format("2006-01-02")

	seededRunID := seedSucceededDoraPartition(t, ctx, store, pool, orgID, closedDay, 0, "post-sync:frozen-run")

	// Sanity: confirm the automatic path really does treat this as terminal
	// coverage on a closed day (CHAOS-4384's documented, intended
	// behavior) -- otherwise this test would not be exercising the gap it
	// claims to.
	repeatTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	scope := json.RawMessage(`{"version":1,"day":"` + closedDay + `","sink":"auto","interval":"daily","backfill_days":1}`)
	repeatRun, err := store.StartRunTx(ctx, repeatTx, StartRunRequest{
		OrganizationID: orgID, Family: "dora",
		Generation: "fixed-schedule:dora_daily_fanout:later", ScopeKey: closedDay,
		Scopes: []json.RawMessage{scope},
	}, nopPartitionPublisher{})
	if err != nil {
		t.Fatal(err)
	}
	_ = repeatTx.Commit(ctx)
	if repeatRun.ID != seededRunID {
		t.Fatalf(
			"test setup invalid: the automatic dora path did NOT treat the closed 0-row day as terminal coverage (got a different run %s than the seeded %s) -- CHAOS-4384's behavior changed underneath this test",
			repeatRun.ID, seededRunID,
		)
	}

	outcome, err := store.StartManualBackfillRun(ctx, "dora", orgID, closedDay, "manual-backfill:test-1", nopPartitionPublisher{})
	if err != nil {
		t.Fatalf("StartManualBackfillRun refused a 0-row closed day: %v (this is exactly the CHAOS-4384 prod shape it exists to recover)", err)
	}
	if outcome.AlreadyRan {
		t.Fatal("expected a genuinely new run, got AlreadyRan=true")
	}
	if outcome.RunID == "" || outcome.RunID == seededRunID {
		t.Fatalf("expected a NEW run distinct from the seeded 0-row run %s, got %q", seededRunID, outcome.RunID)
	}
	if outcome.PartitionID == "" {
		t.Fatal("expected a non-empty partition id")
	}

	claim, err := store.ClaimPartition(ctx, outcome.PartitionID)
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil {
		t.Fatal("the manual backfill run's own partition is not claimable -- it was not really a fresh dispatch")
	}

	var runCount int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM remaining_metric_runs WHERE org_id = $1::uuid AND family = 'dora'",
		orgID,
	).Scan(&runCount); err != nil {
		t.Fatal(err)
	}
	if runCount != 2 {
		t.Fatalf("expected 2 dora runs (the frozen 0-row seed plus the manual recompute), found %d", runCount)
	}
}

func TestStartManualBackfillRunRefusesANonZeroRowCoveredDay(t *testing.T) {
	// Red-on-baseline for the guard: a day that already has REAL output must
	// never be silently duplicated. dora_metrics_daily is append-only with
	// no dedup on replay (CHAOS-4242) -- a second insert for an
	// already-covered day is a genuine duplicate, not a re-run.
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
	createRemainingTables(t, ctx, pool)

	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	const orgID = "00000000-0000-4000-8000-000000004255"
	day := time.Now().UTC().AddDate(0, 0, -3).Format("2006-01-02")

	seededRunID := seedSucceededDoraPartition(t, ctx, store, pool, orgID, day, 5, "post-sync:real-data-run")

	outcome, err := store.StartManualBackfillRun(ctx, "dora", orgID, day, "manual-backfill:test-2", nopPartitionPublisher{})
	if !errors.Is(err, ErrDayAlreadyCovered) {
		t.Fatalf("expected ErrDayAlreadyCovered for a non-zero-row covered day, got err=%v outcome=%+v", err, outcome)
	}
	if outcome.RunID != seededRunID {
		t.Fatalf("expected the refusal to report the existing covering run %s, got %q", seededRunID, outcome.RunID)
	}

	var runCount int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM remaining_metric_runs WHERE org_id = $1::uuid AND family = 'dora'",
		orgID,
	).Scan(&runCount); err != nil {
		t.Fatal(err)
	}
	if runCount != 1 {
		t.Fatalf("expected no new run to be inserted (still 1), found %d", runCount)
	}
}

func TestStartManualBackfillRunIsIdempotentForARepeatedGeneration(t *testing.T) {
	// A retried CLI invocation that reuses the same minted generation (e.g.
	// an operator re-running the exact same command after a network blip)
	// must reuse the same run, not fail or insert a duplicate.
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
	createRemainingTables(t, ctx, pool)

	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	const orgID = "00000000-0000-4000-8000-000000004256"
	day := time.Now().UTC().AddDate(0, 0, -3).Format("2006-01-02")
	const generation = "manual-backfill:2026-08-28T00:00:00Z"

	first, err := store.StartManualBackfillRun(ctx, "dora", orgID, day, generation, nopPartitionPublisher{})
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if first.AlreadyRan {
		t.Fatal("first call reported AlreadyRan=true")
	}

	second, err := store.StartManualBackfillRun(ctx, "dora", orgID, day, generation, nopPartitionPublisher{})
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if !second.AlreadyRan {
		t.Fatal("second call with the same generation did not report AlreadyRan=true")
	}
	if second.RunID != first.RunID {
		t.Fatalf("second call returned a different run id: first=%s second=%s", first.RunID, second.RunID)
	}

	var runCount int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM remaining_metric_runs WHERE org_id = $1::uuid AND family = 'dora'",
		orgID,
	).Scan(&runCount); err != nil {
		t.Fatal(err)
	}
	if runCount != 1 {
		t.Fatalf("expected exactly 1 run across both calls, found %d", runCount)
	}
}

// TestStartManualBackfillRunDetectsCoverageFromAWiderMultiDayRun is
// red-on-baseline for codex review's P1 finding: DORAExecutor.ComputePartition
// writes rows for EVERY day in [anchor-backfill_days+1, anchor] from ONE
// partition anchored on the LATEST day, not one partition per day. A
// same-day-only coverage check misses this entirely for any day strictly
// before the anchor.
func TestStartManualBackfillRunDetectsCoverageFromAWiderMultiDayRun(t *testing.T) {
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
	createRemainingTables(t, ctx, pool)

	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	const orgID = "00000000-0000-4000-8000-000000004257"

	// A post-sync catch-up run anchored 2026-08-27 with backfill_days=3
	// covers 2026-08-25, 2026-08-26, and 2026-08-27 -- one partition, one
	// row per day (or more), no separate partition for the interior days.
	anchorDay := "2026-08-27"
	interiorDay := "2026-08-25"
	seededRunID := seedSucceededDoraPartitionWithBackfillDays(
		t, ctx, store, pool, orgID, anchorDay, 3, 7, "post-sync:wide-catchup",
	)

	outcome, err := store.StartManualBackfillRun(ctx, "dora", orgID, interiorDay, "manual-backfill:test-3", nopPartitionPublisher{})
	if !errors.Is(err, ErrDayAlreadyCovered) {
		t.Fatalf(
			"expected ErrDayAlreadyCovered for %s (covered by the %s anchor's 3-day window), got err=%v outcome=%+v",
			interiorDay, anchorDay, err, outcome,
		)
	}
	if outcome.RunID != seededRunID {
		t.Fatalf("expected the refusal to report the wide-window run %s, got %q", seededRunID, outcome.RunID)
	}

	var runCount int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM remaining_metric_runs WHERE org_id = $1::uuid AND family = 'dora'",
		orgID,
	).Scan(&runCount); err != nil {
		t.Fatal(err)
	}
	if runCount != 1 {
		t.Fatalf("expected no duplicate run to be inserted (still 1), found %d", runCount)
	}
}

// TestStartManualBackfillRunAllowsADayOutsideAWiderRunsWindow is the
// companion negative case: a day just outside the wide run's window must
// still be treated as uncovered.
func TestStartManualBackfillRunAllowsADayOutsideAWiderRunsWindow(t *testing.T) {
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
	createRemainingTables(t, ctx, pool)

	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	const orgID = "00000000-0000-4000-8000-000000004258"

	// Anchored 2026-08-27, backfill_days=3 covers 08-25..08-27. 08-24 is
	// one day outside that window and must still be backfillable.
	seedSucceededDoraPartitionWithBackfillDays(
		t, ctx, store, pool, orgID, "2026-08-27", 3, 7, "post-sync:wide-catchup",
	)

	outcome, err := store.StartManualBackfillRun(ctx, "dora", orgID, "2026-08-24", "manual-backfill:test-4", nopPartitionPublisher{})
	if err != nil {
		t.Fatalf("day just outside the wide run's window was refused: %v (outcome=%+v)", err, outcome)
	}
	if outcome.AlreadyRan {
		t.Fatal("expected a genuinely new run")
	}
}
