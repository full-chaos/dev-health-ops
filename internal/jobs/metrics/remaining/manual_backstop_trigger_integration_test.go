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

// CHAOS-5016: `metrics remaining trigger-backstop`'s core, StartManualBackfillRun
// called with family="work_item_attribution", reuses the exact same store
// path manual_backfill_integration_test.go already covers for dora -- these
// tests are scoped to the two properties that are NEW for
// work_item_attribution rather than re-proving the whole shared mechanism:
//
//  1. work_item_attribution's scope carries no "day" field at all (a static
//     {"version":1,"org_wide":true} placeholder), which is exactly the shape
//     findManualBackfillBlocker's query used to silently mishandle (it
//     filtered on partition.scope->>'day' alone, NULL for this family) before
//     it gained a COALESCE(partition.scope->>'day', run.scope_key) fallback
//     -- these tests are the red-on-baseline proof that fix actually closes
//     the coverage gap for this family, without changing dora/complexity/
//     release_impact's behavior (their scope always has a real "day", so
//     COALESCE resolves to the exact same value the old query read).
//  2. the deliberate NON-goal: proving trigger-backstop never touches
//     fixed_schedule_occurrences is a STRUCTURAL fact, not a runtime one --
//     `go list -deps ./internal/jobs/metrics/remaining` has no edge to
//     internal/scheduler/fixed at all (confirmed in the PR body), so nothing
//     in this package can reference that table's writer. No test asserts it
//     here because the type system already forbids it more strongly than a
//     row-count assertion could.

// seedSucceededWorkItemAttributionPartition starts and completes one
// automatic-trigger-shaped work_item_attribution run/partition for
// (orgID, day) with the given rows_written, mirroring
// seedSucceededDoraPartition's shape but with work_item_attribution's own
// static, day-less scope (producers.go's work_item_attribution_daily_fanout
// binding). Returns the seeded run's id.
func seedSucceededWorkItemAttributionPartition(
	t *testing.T, ctx context.Context, store *PostgresStore, orgID, day string, rowsWritten int, generation string,
) string {
	t.Helper()
	scope := json.RawMessage(`{"version":1,"org_wide":true}`)
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.StartRunTx(ctx, tx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "work_item_attribution",
		Generation:     generation,
		ScopeKey:       day,
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

// TestFindManualBackfillBlockerDetectsWorkItemAttributionCoverageViaScopeKey
// is the red-on-baseline proof for the COALESCE(partition.scope->>'day',
// run.scope_key) fix: before it, findManualBackfillBlocker's query filtered
// on partition.scope->>'day' alone, which is NULL for work_item_attribution's
// scope (no "day" field) -- every row for this family was silently excluded
// from the WHERE clause, so this coverage check would have found NOTHING no
// matter how many succeeded partitions already existed, and a repeat trigger
// would have inserted a genuine duplicate run for an already-covered day.
func TestFindManualBackfillBlockerDetectsWorkItemAttributionCoverageViaScopeKey(t *testing.T) {
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
	const orgID = "00000000-0000-4000-8000-000000005016"
	day := time.Now().UTC().AddDate(0, 0, -3).Format("2006-01-02")

	seededRunID := seedSucceededWorkItemAttributionPartition(t, ctx, store, orgID, day, 12, "fixed-schedule:work_item_attribution_daily_fanout:earlier")

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	blockingRunID, reason, err := store.findManualBackfillBlocker(ctx, tx, orgID, "work_item_attribution", day, "manual-trigger:work_item_attribution:"+orgID+":"+day)
	if err != nil {
		t.Fatal(err)
	}
	if reason != blockReasonCovered {
		t.Fatalf("expected blockReasonCovered for an already-succeeded work_item_attribution day, got reason=%v (this is exactly the scope->>'day' vs scope_key gap the fix closes)", reason)
	}
	if blockingRunID != seededRunID {
		t.Fatalf("expected the blocker to name the seeded run %s, got %q", seededRunID, blockingRunID)
	}
}

// TestStartManualBackfillRunWorkItemAttributionRefusesARepeatTriggerForACoveredDay
// is trigger-backstop's own idempotency requirement end to end: a repeat
// manual trigger for an org/day that already has a succeeded
// work_item_attribution run must be refused, not inserted as a second live
// run -- ComputeOrg is watermark-driven and safe to re-run, but the run
// LEDGER itself must still not silently duplicate.
func TestStartManualBackfillRunWorkItemAttributionRefusesARepeatTriggerForACoveredDay(t *testing.T) {
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
	const orgID = "00000000-0000-4000-8000-000000005017"
	day := time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")

	// Seed as if the automatic 02:30Z occurrence already ran and succeeded
	// for this org/day.
	seededRunID := seedSucceededWorkItemAttributionPartition(t, ctx, store, orgID, day, 7, "fixed-schedule:work_item_attribution_daily_fanout:tonight")

	// An operator triggers it manually anyway -- under a DIFFERENT
	// generation, as a real second invocation would be (CHAOS-5016's own
	// manualBackstopTriggerGeneration is deterministic per (family,org,day),
	// but this proves the refusal does not depend on that -- it holds even
	// for a distinct generation string).
	outcome, startErr := store.StartManualBackfillRun(ctx, "work_item_attribution", orgID, day, "manual-trigger:work_item_attribution:"+orgID+":"+day+":second-attempt", nopPartitionPublisher{})
	if !errors.Is(startErr, ErrDayAlreadyCovered) {
		t.Fatalf("expected ErrDayAlreadyCovered for an already-covered day, got err=%v outcome=%+v", startErr, outcome)
	}
	if outcome.RunID != seededRunID {
		t.Fatalf("expected the refusal to report the existing covering run %s, got %q", seededRunID, outcome.RunID)
	}

	var runCount int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM remaining_metric_runs WHERE org_id = $1::uuid AND family = 'work_item_attribution'",
		orgID,
	).Scan(&runCount); err != nil {
		t.Fatal(err)
	}
	if runCount != 1 {
		t.Fatalf("expected no new run inserted for the repeat trigger (still 1), found %d", runCount)
	}
}

// TestStartManualBackfillRunWorkItemAttributionIsIdempotentForARepeatedGeneration
// mirrors TestStartManualBackfillRunIsIdempotentForARepeatedGeneration
// (dora) for work_item_attribution: a retried CLI invocation reusing the
// SAME minted generation (manualBackstopTriggerGeneration's whole point --
// it is a pure function of family/org/day, not wall-clock time) must reuse
// the same in-flight run, not fail or insert a duplicate.
func TestStartManualBackfillRunWorkItemAttributionIsIdempotentForARepeatedGeneration(t *testing.T) {
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
	const orgID = "00000000-0000-4000-8000-000000005018"
	day := time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")
	const generation = "manual-trigger:work_item_attribution:" + orgID + ":" + "same-day"

	first, err := store.StartManualBackfillRun(ctx, "work_item_attribution", orgID, day, generation, nopPartitionPublisher{})
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if first.AlreadyRan {
		t.Fatal("first call reported AlreadyRan=true")
	}

	second, err := store.StartManualBackfillRun(ctx, "work_item_attribution", orgID, day, generation, nopPartitionPublisher{})
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
		"SELECT count(*) FROM remaining_metric_runs WHERE org_id = $1::uuid AND family = 'work_item_attribution'",
		orgID,
	).Scan(&runCount); err != nil {
		t.Fatal(err)
	}
	if runCount != 1 {
		t.Fatalf("expected exactly 1 run across both calls, found %d", runCount)
	}
}

// TestStartManualBackfillRunWorkItemAttributionCoexistsWithASameDayAutomaticOccurrence
// answers team-lead's review question precisely (CHAOS-5016): a manual
// trigger-backstop run does NOT suppress the schedule's own 02:30Z
// occurrence, even on the (rare) day their ScopeKeys happen to match. It
// proves this at the exact mechanism level: StartRunTx's coverage-merging
// advisory-lock block (postgres.go) applies ONLY to family=="dora" -- for
// work_item_attribution the automatic path falls straight through to
// insertRun's plain (org_id, family, generation, scope_key) ON CONFLICT DO
// NOTHING, and the automatic generation ("fixed-schedule:..." ) never
// equals this command's ("manual-trigger:..."), so the two dispatches never
// collide on that constraint -- they insert as two independent runs, each
// with its own claimable partition. This is the same-day edge case; the
// TYPICAL case (--day defaults to yesterday, tonight's occurrence is
// tomorrow's UTC date relative to a PDT-afternoon trigger) never even
// reaches this shared-key scenario.
func TestStartManualBackfillRunWorkItemAttributionCoexistsWithASameDayAutomaticOccurrence(t *testing.T) {
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
	const orgID = "00000000-0000-4000-8000-000000005019"
	// The SAME day for both dispatches -- the scenario team-lead's question
	// is actually about. An operator running trigger-backstop --today right
	// before the schedule's own 02:30Z boundary for that same UTC date is
	// the only realistic way these ScopeKeys ever match.
	day := time.Now().UTC().Format("2006-01-02")

	// Same generation SHAPE cmd/dev-health-workerctl's manualBackstopTriggerGeneration
	// mints ("manual-trigger:<family>:<org>:<day>") -- that helper lives in
	// package main and is not importable from here, but the exact string is
	// not load-bearing for this test, only that it differs from the
	// automatic path's "fixed-schedule:..." generation below.
	manualGeneration := "manual-trigger:work_item_attribution:" + orgID + ":" + day
	manualOutcome, err := store.StartManualBackfillRun(ctx, "work_item_attribution", orgID, day, manualGeneration, nopPartitionPublisher{})
	if err != nil {
		t.Fatalf("manual trigger: %v", err)
	}
	if manualOutcome.RunID == "" {
		t.Fatal("manual trigger returned an empty run id")
	}

	// Now simulate the schedule's OWN dispatch for the identical org+day,
	// via StartRunTx exactly as RemainingMetricsFanoutProducer.Produce
	// (internal/scheduler/fixed/producers.go) calls it -- same scope shape,
	// same generation format, NOT through StartManualBackfillRun/
	// findManualBackfillBlocker at all (the automatic path never calls
	// either).
	scope := json.RawMessage(`{"version":1,"org_wide":true}`)
	automaticGeneration := "fixed-schedule:work_item_attribution_daily_fanout:" + time.Now().UTC().Format(time.RFC3339)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	automaticRun, err := store.StartRunTx(ctx, tx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "work_item_attribution",
		Generation:     automaticGeneration,
		ScopeKey:       day,
		Scopes:         []json.RawMessage{scope},
	}, nopPartitionPublisher{})
	if err != nil {
		t.Fatalf("automatic occurrence was blocked/errored by the earlier manual trigger for the same day: %v (expected: coexist, not suppress)", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	if automaticRun.ID == manualOutcome.RunID {
		t.Fatalf("expected the automatic occurrence to create a DISTINCT run, got the SAME run id as the manual trigger (%s) -- that would mean the manual trigger silently absorbed/suppressed the real occurrence", automaticRun.ID)
	}

	// Both runs' partitions are independently claimable -- neither one was
	// merged into or superseded by the other.
	manualClaim, err := store.ClaimPartition(ctx, manualOutcome.PartitionID)
	if err != nil {
		t.Fatal(err)
	}
	if manualClaim == nil {
		t.Fatal("the manual trigger's own partition is not claimable")
	}
	automaticPartitionID := deterministicPartitionID(automaticRun.ID, 1)
	automaticClaim, err := store.ClaimPartition(ctx, automaticPartitionID)
	if err != nil {
		t.Fatal(err)
	}
	if automaticClaim == nil {
		t.Fatal("the automatic occurrence's own partition is not claimable")
	}

	var runCount int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM remaining_metric_runs WHERE org_id = $1::uuid AND family = 'work_item_attribution'",
		orgID,
	).Scan(&runCount); err != nil {
		t.Fatal(err)
	}
	if runCount != 2 {
		t.Fatalf("expected exactly 2 independent runs (manual + automatic), found %d", runCount)
	}
}
