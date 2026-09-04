//go:build integration

package daily

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestStartManualDailyRunWithDeferredDiscoveryMaterializesThroughTheSharedPath
// is the red-on-baseline proof for codex adversarial review round 1, P1
// (CHAOS-5055): `dev-hops metrics daily --org O` without --repo-id -- the
// CLI's own default, documented path ("Omit for every org repository
// (deferred discovery, resolved by the worker)", workerctl_dispatch.py) --
// dispatches a manual-daily run with zero partitions, exactly like the
// nightly fixed schedule and post-sync re-drive already do. Before this fix,
// MaterializeScheduledFanout only accepted a scheduled-fanout or post-sync
// generation, so the worker's Dispatcher.Work treated the resulting
// ErrInvalidState as jobruntime.Permanent (daily.go:325-335) and the run was
// stranded 'running' forever with no partitions, no completion fence, and no
// re-drive path -- despite RepositoryDiscoveryRequired correctly reporting
// true and live ClickHouse discovery succeeding. isManualDailyGeneration
// closes that gap: this test proves StartManualDailyRun -> ClaimDispatch ->
// MaterializeScheduledFanout now succeeds end to end for the exact shape a
// default `metrics daily-start` invocation produces.
func TestStartManualDailyRunWithDeferredDiscoveryMaterializesThroughTheSharedPath(t *testing.T) {
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

	const org = "00000000-0000-4000-8000-000000000011"
	const day = "2026-08-26"
	generation := ManualDailyRunGeneration(org, day, nil)

	// repositoryIDs=nil is the CLI's default: `dev-hops metrics daily --org
	// O` with no --repo-id flags.
	outcome, err := store.StartManualDailyRun(ctx, org, day, generation, nil, publisher)
	if err != nil {
		t.Fatalf("StartManualDailyRun (deferred discovery) failed: %v", err)
	}

	var partitionsBeforeDiscovery int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_partitions WHERE run_id = $1::uuid`, outcome.RunID).Scan(&partitionsBeforeDiscovery); err != nil {
		t.Fatal(err)
	}
	if partitionsBeforeDiscovery != 0 {
		t.Fatalf("manual-daily run created %d partitions before discovery ran; want 0 (deferred discovery)", partitionsBeforeDiscovery)
	}

	claimed, err := store.ClaimDispatch(ctx, outcome.RunID)
	if err != nil || claimed == nil || !claimed.RepositoryDiscoveryRequired {
		t.Fatalf("manual-daily dispatch claim=%#v err=%v, want RepositoryDiscoveryRequired=true", claimed, err)
	}

	// The heavy worker resolves this against ClickHouse repos.id in
	// production (daily.RepositoryDiscoverer); this store-layer test
	// supplies the discovered set directly, exactly like the post-sync
	// deferred-discovery test above -- MaterializeScheduledFanout is where
	// this fix's store-side contract lives.
	discovered := []RepositoryID{
		"00000000-0000-4000-8000-000000000002",
		"00000000-0000-4000-8000-000000000001",
	}
	created, err := store.MaterializeScheduledFanout(ctx, *claimed, discovered)
	if err != nil || !created {
		t.Fatalf("manual-daily deferred-discovery materialize=%t err=%v -- before the CHAOS-5055 fix this returned ErrInvalidState, permanently stranding the run", created, err)
	}

	var partitionCount int
	var ids string
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_partitions WHERE run_id = $1::uuid`, outcome.RunID).Scan(&partitionCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT repo_ids::text FROM daily_metrics_partitions WHERE run_id = $1::uuid`, outcome.RunID).Scan(&ids); err != nil {
		t.Fatal(err)
	}
	if partitionCount != 1 || ids != `["00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002"]` {
		t.Fatalf("manual-daily materialized partitions=%d ids=%s", partitionCount, ids)
	}
}
