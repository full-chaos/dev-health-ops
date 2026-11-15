//go:build integration

package daily

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// C4: the whole path against a real Postgres. A zero-row partition ends
// failed_permanent with its reason; the blocked-run marker then sees the run, the
// counter's source (Marked) moves, and the operator readback names the reason.
// Before CHAOS-13863 the partition ended 'failed' with no reason and the run
// stayed 'running' with no signal.
func TestZeroRowPartitionEndsFailedPermanentAndTheBlockedMarkerSeesTheRun(t *testing.T) {
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
	now := time.Now().UTC().Truncate(time.Microsecond)
	seedBlockedRun(t, ctx, pool, blockedFixture{
		runID: runID, orgID: orgID, partitionState: []string{"succeeded", "pending"},
	}, now)
	var partitionID string
	if err := pool.QueryRow(ctx,
		`SELECT id::text FROM daily_metrics_partitions WHERE run_id = $1::uuid AND status = 'pending'`, runID,
	).Scan(&partitionID); err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewPartitionHandler(store, fakePublisher{})
	if err != nil {
		t.Fatal(err)
	}
	handler.SetSourceDataChecker(familiesSourceDataChecker{families: []string{"cicd"}})

	if err := handler.Work(ctx, partitionExecutionFor(partitionID, runID, orgID)); !errors.Is(err, ErrZeroRowsWithSourceData) {
		t.Fatalf("Work error = %v, want ErrZeroRowsWithSourceData", err)
	}
	var status string
	var reason *string
	if err := pool.QueryRow(ctx,
		`SELECT status, failure_reason FROM daily_metrics_partitions WHERE id = $1::uuid`, partitionID,
	).Scan(&status, &reason); err != nil {
		t.Fatal(err)
	}
	if status != "failed_permanent" || reason == nil || *reason != "zero_rows_with_source_data" {
		t.Fatalf("partition = (%s, %v), want (failed_permanent, zero_rows_with_source_data)", status, reason)
	}

	result, err := store.ReconcileBlockedRuns(ctx, orgID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Marked != 1 || result.Blocked != 1 {
		t.Fatalf("reconcile = %+v, want marked 1 / blocked 1", result)
	}
	if blocked, why := readMarker(t, ctx, pool, runID); !blocked || why != BlockedReasonPartialPartitionsPermanent {
		t.Fatalf("marker = (%v, %q), want (true, %s)", blocked, why, BlockedReasonPartialPartitionsPermanent)
	}
	listed, err := store.BlockedRuns(ctx, orgID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].RunID != runID || len(listed[0].FailureReasons) != 1 ||
		listed[0].FailureReasons[0] != "zero_rows_with_source_data" {
		t.Fatalf("readback = %+v, want the run with failure reason zero_rows_with_source_data", listed)
	}
}
