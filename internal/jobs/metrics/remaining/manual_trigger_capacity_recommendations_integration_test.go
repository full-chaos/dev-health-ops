//go:build integration

package remaining

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// completeFirstPartitionWithRowsWritten claims and completes run's sole
// partition (ordinal 1) with the given rows_written, in
// compatibilityCompletionResult's real evidence shape -- shared scaffolding
// for the collision-regression tests below.
func completeFirstPartitionWithRowsWritten(t *testing.T, ctx context.Context, store *PostgresStore, runID string, rowsWritten int) {
	t.Helper()
	partitionID := deterministicPartitionID(runID, 1)
	claim, err := store.ClaimPartition(ctx, partitionID)
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil {
		t.Fatalf("run %s partition was not claimable", runID)
	}
	evidence := "compatibility_execution:" + partitionID + ":rows_written=" + strconv.Itoa(rowsWritten)
	if err := store.CompletePartition(ctx, *claim, evidence); err != nil {
		t.Fatal(err)
	}
}

// TestStartManualCapacityTriggerRunDoesNotCollideAcrossTeams is the
// red-on-baseline proof for codex adversarial review round 1, P1
// (CHAOS-5055): before this fix, findManualBackfillBlocker's coverage query
// filtered only on (org, family, day-range) -- never on partition.scope's
// team_id -- so team B's capacity trigger for the SAME org/day as team A's
// already-succeeded trigger read back as ErrDayAlreadyCovered, silently
// never computing team B's forecast at all, and reporting exit 0 as if it
// had.
func TestStartManualCapacityTriggerRunDoesNotCollideAcrossTeams(t *testing.T) {
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
	const orgID = "00000000-0000-4000-8000-000000005055"
	const teamA = "00000000-0000-4000-8000-00000000a001"
	const teamB = "00000000-0000-4000-8000-00000000a002"
	day := time.Now().UTC().AddDate(0, 0, -3).Format("2006-01-02")

	teamAID, teamBID := teamA, teamB

	outcomeA, err := store.StartManualCapacityTriggerRun(ctx, orgID, day, "manual-trigger:capacity:"+orgID+":"+day+":team:"+teamA, &teamAID, false, nopPartitionPublisher{})
	if err != nil {
		t.Fatalf("team A trigger failed: %v", err)
	}
	completeFirstPartitionWithRowsWritten(t, ctx, store, outcomeA.RunID, 12)

	outcomeB, err := store.StartManualCapacityTriggerRun(ctx, orgID, day, "manual-trigger:capacity:"+orgID+":"+day+":team:"+teamB, &teamBID, false, nopPartitionPublisher{})
	if err != nil {
		t.Fatalf("team B trigger incorrectly blocked by team A's succeeded run: %v (outcome=%+v)", err, outcomeB)
	}
	if outcomeB.RunID == outcomeA.RunID {
		t.Fatalf("team B was given team A's run id %s -- the two teams' forecasts collided onto one row", outcomeA.RunID)
	}

	var runCount int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM remaining_metric_runs WHERE org_id = $1::uuid AND family = 'capacity'",
		orgID,
	).Scan(&runCount); err != nil {
		t.Fatal(err)
	}
	if runCount != 2 {
		t.Fatalf("expected 2 distinct capacity runs (one per team), found %d", runCount)
	}
}

// TestStartManualRecommendationsTriggerRunDoesNotCollideAcrossWindows mirrors
// the capacity test above for recommendations' second real scope axis:
// evaluation window. Same team, same org, same day, two different windows --
// both must compute, neither may read the other as coverage.
func TestStartManualRecommendationsTriggerRunDoesNotCollideAcrossWindows(t *testing.T) {
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
	const orgID = "00000000-0000-4000-8000-000000005056"
	const team = "00000000-0000-4000-8000-00000000b001"
	day := time.Now().UTC().AddDate(0, 0, -3).Format("2006-01-02")
	teamID := team

	outcome7, err := store.StartManualRecommendationsTriggerRun(ctx, orgID, day, "manual-trigger:recommendations:"+orgID+":"+day+":window:7", &teamID, 7, nopPartitionPublisher{})
	if err != nil {
		t.Fatalf("window=7 trigger failed: %v", err)
	}
	completeFirstPartitionWithRowsWritten(t, ctx, store, outcome7.RunID, 3)

	outcome30, err := store.StartManualRecommendationsTriggerRun(ctx, orgID, day, "manual-trigger:recommendations:"+orgID+":"+day+":window:30", &teamID, 30, nopPartitionPublisher{})
	if err != nil {
		t.Fatalf("window=30 trigger incorrectly blocked by window=7's succeeded run: %v (outcome=%+v)", err, outcome30)
	}
	if outcome30.RunID == outcome7.RunID {
		t.Fatalf("window=30 was given window=7's run id %s -- the two windows' computations collided onto one row", outcome7.RunID)
	}

	var runCount int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM remaining_metric_runs WHERE org_id = $1::uuid AND family = 'recommendations'",
		orgID,
	).Scan(&runCount); err != nil {
		t.Fatal(err)
	}
	if runCount != 2 {
		t.Fatalf("expected 2 distinct recommendations runs (one per window), found %d", runCount)
	}
}
