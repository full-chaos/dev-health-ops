//go:build integration

package remaining

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestClaimPartitionReturnsScope is the narrow, fast regression pin for
// CHAOS-4242: PostgresStore.ClaimPartition's RETURNING clause used to select
// id/run_id/ordinal/claim_token and NOT scope, so every real claim handed
// callers a Claim whose Partition.Scope was an empty json.RawMessage.
//
// HTTPCompatibilityExecutor.ComputePartition never notices -- it posts
// run_id/partition_id to the Python bridge and the bridge re-loads scope
// itself, so the five compatibility kinds stayed green. DORAExecutor and
// CapacityExecutor are the only two ComputePartition implementations that
// decode Partition.Scope directly (dora_native.go, capacity_native.go), so
// they were the only two kinds this defect could ever surface on -- which is
// exactly the CHAOS-4242 symptom shape (both native kinds broken, all five
// bridge kinds healthy, ~10ms failures, before any ClickHouse read/write).
//
// This test asserts the claimed scope round-trips byte-for-byte against a
// real Postgres, independent of any downstream executor, so it pins the
// Store contract every ComputePartition implementation depends on -- not
// just DORA's.
func TestClaimPartitionReturnsScope(t *testing.T) {
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

	const orgID = "00000000-0000-4000-8000-000000004242"
	wantScope := json.RawMessage(`{"version":1,"day":"2026-08-24","sink":"auto","interval":"daily","backfill_days":1}`)
	run, err := store.StartRun(ctx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "dora",
		Generation:     "dora-v1",
		ScopeKey:       "all-repos",
		Scopes:         []json.RawMessage{wantScope},
	})
	if err != nil {
		t.Fatal(err)
	}
	partitionID := deterministicPartitionID(run.ID, 1)

	claim, err := store.ClaimPartition(ctx, partitionID)
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil {
		t.Fatal("partition was not claimable")
	}
	gotScope, err := canonicalJSON(claim.Partition.Scope)
	if err != nil {
		t.Fatalf(
			"ClaimPartition returned an unusable scope (%q): %v -- this is the "+
				"exact defect CHAOS-4242 traced: a claim with no scope makes "+
				"json.Unmarshal fail in a native ComputePartition before any "+
				"ClickHouse read or write ever happens",
			claim.Partition.Scope, err,
		)
	}
	wantCanonical, err := canonicalJSON(wantScope)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotScope) != string(wantCanonical) {
		t.Fatalf("ClaimPartition scope = %s, want %s", gotScope, wantCanonical)
	}

	// PendingPartitions is the OTHER Partition-returning method -- it always
	// selected scope correctly (postgres.go), which is exactly why it was
	// never suspected: ClaimPartition was the only Store method callers
	// actually reach through PartitionHandler.Work. Table-driven over both
	// so the next method added to Store that returns a Partition is caught
	// here rather than rediscovered live.
	secondScope := json.RawMessage(`{"version":1,"day":"2026-08-25","sink":"auto","interval":"daily","backfill_days":1}`)
	secondRun, err := store.StartRun(ctx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "dora",
		Generation:     "dora-v2",
		ScopeKey:       "all-repos",
		Scopes:         []json.RawMessage{secondScope},
	})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingPartitions(ctx, secondRun.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("PendingPartitions returned %d partitions, want 1", len(pending))
	}
	pendingScope, err := canonicalJSON(pending[0].Scope)
	if err != nil {
		t.Fatalf("PendingPartitions returned an unusable scope: %v", err)
	}
	wantSecondCanonical, err := canonicalJSON(secondScope)
	if err != nil {
		t.Fatal(err)
	}
	if string(pendingScope) != string(wantSecondCanonical) {
		t.Fatalf("PendingPartitions scope = %s, want %s", pendingScope, wantSecondCanonical)
	}
}

// TestFailedPartitionIsReclaimableWithScopeIntact is the CHAOS-4242
// stale-window recovery proof. Prod has had no DORA rows since 2026-08-19:
// every affected day's post-sync dispatch already created a
// remaining_metric_partitions row, attempted it 3 times, and left it
// status='failed' when the discard happened -- it never self-deletes.
// ClaimPartition's own WHERE clause already treats 'failed' as reclaimable
// (status IN ('pending','failed')), so once this fix ships, retrying any of
// those discarded River jobs (dev-health-workerctl's existing `jobs retry`
// command, StateDiscarded is retry-eligible per
// internal/joboperator/service.go's retryEligible) re-claims the SAME
// partition row -- with its scope now intact -- and writes the missing
// day's rows for the FIRST and only time. No new backfill machinery, no
// duplicate-write risk: these partitions never wrote anything the first
// time, because they always failed before reaching ComputePartition's
// ClickHouse calls.
func TestFailedPartitionIsReclaimableWithScopeIntact(t *testing.T) {
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

	const orgID = "00000000-0000-4000-8000-000000004242"
	staleScope := json.RawMessage(`{"version":1,"day":"2026-08-19","sink":"auto","interval":"daily","backfill_days":1}`)
	run, err := store.StartRun(ctx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "dora",
		Generation:     "post-sync:historical-sync-run-id",
		ScopeKey:       string(staleScope),
		Scopes:         []json.RawMessage{staleScope},
	})
	if err != nil {
		t.Fatal(err)
	}
	partitionID := deterministicPartitionID(run.ID, 1)

	// Simulate the pre-fix history: claimed, computed, failed, 3 times.
	if _, err := pool.Exec(ctx, `
UPDATE public.remaining_metric_partitions
SET status = 'failed', claim_token = NULL, lease_expires_at = NULL, attempt_count = 3
WHERE id = $1::uuid`, partitionID); err != nil {
		t.Fatal(err)
	}

	reclaimed, err := store.ClaimPartition(ctx, partitionID)
	if err != nil {
		t.Fatal(err)
	}
	if reclaimed == nil {
		t.Fatal("a failed partition from before the fix was not reclaimable -- the historical gap can never self-heal")
	}
	gotScope, err := canonicalJSON(reclaimed.Partition.Scope)
	if err != nil {
		t.Fatalf("reclaimed partition scope is unusable: %v", err)
	}
	wantCanonical, err := canonicalJSON(staleScope)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotScope) != string(wantCanonical) {
		t.Fatalf("reclaimed scope = %s, want %s", gotScope, wantCanonical)
	}
	if err := store.CompletePartition(ctx, *reclaimed, "rows=1;sha256=recovered"); err != nil {
		t.Fatalf("reclaimed partition did not complete: %v", err)
	}
}

// TestDORAExecutorComputesThroughTheRealClaimPath is the CHAOS-4242
// reachability proof: it does not call DORAExecutor.ComputePartition
// directly (that always worked -- see the investigation trail on the
// ticket). It drives the exact production route -- PartitionHandler.Work,
// which calls Store.ClaimPartition before it ever reaches the executor --
// against real Postgres and a fully migrated real ClickHouse, and asserts a
// row actually lands in dora_metrics_daily and is readable back. Before the
// CHAOS-4242 fix this fails at the ClaimPartition boundary, never reaching
// ClickHouse at all -- exactly as prod did, 88 times, discarded on attempt
// 3 every time.
func TestDORAExecutorComputesThroughTheRealClaimPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pgInstance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pgInstance.Close(context.Background())
	pool, err := pgxpool.New(ctx, pgInstance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createRemainingTables(t, ctx, pool)

	conn := migratedClickHouse(t, ctx, OperationalOrderingLegacy)

	const orgID = "00000000-0000-4000-8000-000000004242"
	repoID := "11111111-2222-4333-8444-555555555555"
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	deployedAt := day.Add(9 * time.Hour)

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO deployments (repo_id, deployment_id, status, environment, started_at, finished_at, deployed_at, merged_at, pull_request_number, release_ref, release_ref_confidence, org_id, last_synced)`)
	if err != nil {
		t.Fatalf("prepare deployments batch: %v", err)
	}
	if err := batch.Append(
		repoID, "deploy-4242", "success", "production",
		&deployedAt, &deployedAt, &deployedAt, (*time.Time)(nil),
		nil, "", 0.0, orgID, deployedAt,
	); err != nil {
		t.Fatalf("append deployment: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send deployments batch: %v", err)
	}

	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	scope := json.RawMessage(`{"version":1,"day":"2026-08-24","sink":"auto","interval":"daily","backfill_days":1}`)
	run, err := store.StartRun(ctx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "dora",
		Generation:     "dora-v1",
		ScopeKey:       "all-repos",
		Scopes:         []json.RawMessage{scope},
	})
	if err != nil {
		t.Fatal(err)
	}
	partitionID := deterministicPartitionID(run.ID, 1)

	executor, err := NewDORAExecutor(ctx, conn, nil)
	if err != nil {
		t.Fatalf("NewDORAExecutor: %v", err)
	}
	handler, err := NewPartitionHandler[jobruntime.RemainingDORAArgs](store, executor, "dora")
	if err != nil {
		t.Fatalf("NewPartitionHandler: %v", err)
	}

	args := jobruntime.RemainingDORAArgs{
		EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.RemainingMetricsPartitionPayload]{
			ContractVersion: jobcontract.ContractVersionV1,
			OrganizationID:  strPtr(orgID),
			CorrelationID:   "chaos-4242-repro",
			IdempotencyKey:  "chaos-4242-repro-" + partitionID,
			Domain: jobcontract.DomainLink{
				Type: "remaining_metric_partition",
				ID:   partitionID,
			},
			Payload: jobcontract.NewRemainingMetricsPartitionPayload(
				jobcontract.KindRemainingDORA, partitionID),
		},
	}
	execution := &jobruntime.Execution[jobruntime.RemainingDORAArgs]{
		JobID:          1,
		Attempt:        1,
		Args:           args,
		Envelope:       args.ContractEnvelope(),
		CorrelationID:  "chaos-4242-repro",
		OrganizationID: strPtr(orgID),
	}

	if err := handler.Work(ctx, execution); err != nil {
		t.Fatalf(
			"PartitionHandler.Work failed through the real claim path: %v -- "+
				"this is the CHAOS-4242 regression: ClaimPartition must return "+
				"a usable scope for a native ComputePartition to ever run",
			err,
		)
	}

	// Readback through the production route: does a row actually land, not
	// just "did ComputePartition return nil".
	var (
		gotRepoID string
		gotOrgID  string
		gotValue  float64
	)
	row := conn.QueryRow(ctx, `
		SELECT repo_id, org_id, value FROM dora_metrics_daily
		WHERE org_id = {org_id:String} AND day = {day:Date} AND metric_name = 'deployment_frequency'`,
		clickhouse.Named("org_id", orgID), clickhouse.Named("day", day.Format("2006-01-02")),
	)
	if err := row.Scan(&gotRepoID, &gotOrgID, &gotValue); err != nil {
		t.Fatalf("readback dora_metrics_daily: %v", err)
	}
	if gotOrgID != orgID {
		t.Fatalf("dora_metrics_daily org_id = %q, want %q", gotOrgID, orgID)
	}
	if gotValue <= 0 {
		t.Fatalf("dora_metrics_daily deployment_frequency = %v, want > 0", gotValue)
	}
}

func strPtr(value string) *string { return &value }

// TestCapacityExecutorComputesThroughTheRealClaimPath is the same CHAOS-4242
// reachability proof as TestDORAExecutorComputesThroughTheRealClaimPath, for
// CapacityExecutor -- the ticket's other affected kind. CapacityExecutor.
// ComputePartition (capacity_native.go:124) decodes Partition.Scope with the
// identical json.Unmarshal(partition.Scope, &scope) call DORAExecutor does,
// and both kinds share the exact same PostgresStore.ClaimPartition, so this
// pins that the CHAOS-4242 fix is not DORA-specific: it is a Store-layer
// fix that both native executors depend on identically.
//
// This seeds one day of real work_item_metrics_daily throughput/backlog and
// reads back through capacity_forecasts -- "does a row land", not merely
// "did ComputePartition return nil" (team-lead review, 2026-08-24: getting
// past the scope decode is not sufficient proof by itself).
func TestCapacityExecutorComputesThroughTheRealClaimPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pgInstance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pgInstance.Close(context.Background())
	pool, err := pgxpool.New(ctx, pgInstance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createRemainingTables(t, ctx, pool)

	conn := migratedClickHouse(t, ctx, OperationalOrderingRevision)

	const orgID = "00000000-0000-4000-8000-000000004242"
	const workScopeID = "chaos-4242-capacity-repro-scope"
	// loadThroughput/loadBacklog window against executor.nowUTC() (real
	// wall-clock, not the scope's own fields), so the seeded day must sit
	// inside [now-history_days, now] of the ACTUAL current time -- "yesterday"
	// rather than a hardcoded date keeps this independent of when the suite
	// runs.
	throughputDay := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_metrics_daily (
day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
items_started_unassigned, items_completed_unassigned, wip_count_end_of_day,
wip_unassigned_end_of_day, bug_completed_ratio, story_points_completed,
computed_at, org_id)`)
	if err != nil {
		t.Fatalf("prepare work_item_metrics_daily batch: %v", err)
	}
	if err := batch.Append(
		throughputDay, "github", workScopeID, "", "",
		uint32(5), uint32(5),
		uint32(0), uint32(0), uint32(10),
		uint32(0), 0.0, 0.0,
		throughputDay, orgID,
	); err != nil {
		t.Fatalf("append work_item_metrics_daily row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_item_metrics_daily batch: %v", err)
	}

	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	scope := json.RawMessage(
		`{"version":1,"all_teams":false,"work_scope_id":"` + workScopeID + `","history_days":90,"simulations":100}`,
	)
	run, err := store.StartRun(ctx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "capacity",
		Generation:     "capacity-v1",
		ScopeKey:       "chaos-4242-scope",
		GenerationSeed: int64Pointer(4242),
		Scopes:         []json.RawMessage{scope},
	})
	if err != nil {
		t.Fatal(err)
	}
	partitionID := deterministicPartitionID(run.ID, 1)

	executor, err := NewCapacityExecutor(ctx, conn, nil)
	if err != nil {
		t.Fatalf("NewCapacityExecutor: %v", err)
	}
	handler, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](store, executor, "capacity")
	if err != nil {
		t.Fatalf("NewPartitionHandler: %v", err)
	}

	args := jobruntime.RemainingCapacityArgs{
		EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.RemainingMetricsPartitionPayload]{
			ContractVersion: jobcontract.ContractVersionV1,
			OrganizationID:  strPtr(orgID),
			CorrelationID:   "chaos-4242-capacity-repro",
			IdempotencyKey:  "chaos-4242-capacity-repro-" + partitionID,
			Domain: jobcontract.DomainLink{
				Type: "remaining_metric_partition",
				ID:   partitionID,
			},
			Payload: jobcontract.NewRemainingMetricsPartitionPayload(
				jobcontract.KindRemainingCapacity, partitionID),
		},
	}
	execution := &jobruntime.Execution[jobruntime.RemainingCapacityArgs]{
		JobID:          1,
		Attempt:        1,
		Args:           args,
		Envelope:       args.ContractEnvelope(),
		CorrelationID:  "chaos-4242-capacity-repro",
		OrganizationID: strPtr(orgID),
	}

	if err := handler.Work(ctx, execution); err != nil {
		t.Fatalf(
			"PartitionHandler.Work failed through the real claim path: %v -- "+
				"CapacityExecutor hits the identical CHAOS-4242 defect DORA "+
				"does: ClaimPartition must return a usable scope",
			err,
		)
	}

	// Readback through the production route: does a row actually land.
	var (
		gotOrgID       string
		gotWorkScopeID string
		gotBacklog     uint32
	)
	row := conn.QueryRow(ctx, `
		SELECT org_id, work_scope_id, backlog_size FROM capacity_forecasts
		WHERE org_id = {org_id:String} AND work_scope_id = {work_scope_id:String}`,
		clickhouse.Named("org_id", orgID), clickhouse.Named("work_scope_id", workScopeID),
	)
	if err := row.Scan(&gotOrgID, &gotWorkScopeID, &gotBacklog); err != nil {
		t.Fatalf("readback capacity_forecasts: %v", err)
	}
	if gotOrgID != orgID || gotWorkScopeID != workScopeID {
		t.Fatalf("capacity_forecasts org_id/work_scope_id = %q/%q, want %q/%q", gotOrgID, gotWorkScopeID, orgID, workScopeID)
	}
	if gotBacklog == 0 {
		t.Fatalf("capacity_forecasts backlog_size = %d, want > 0", gotBacklog)
	}
}

// TestStartRunTxDeduplicatesDoraAcrossGenerationsForTheSameDay is the
// CHAOS-4242 codex-round-2 fix: round 1's SkipIfCovered pre-flight only
// protects the fixed-schedule producer's OWN call to StartRunTx -- it does
// nothing when the post-sync trigger (cmd/dev-health-worker/sync_dispatch.go's
// remainingPostSyncWriter, which never checked coverage at all) lands
// SECOND, and the pre-flight's own read-before-savepoint is itself a TOCTOU
// window a concurrent commit can slip through. Both gaps are closed by
// moving the actual guarantee into StartRunTx itself (postgres.go): a
// transaction-scoped advisory lock keyed on (org, day), taken by EVERY
// StartRunTx call for family "dora" regardless of caller, so whichever call
// commits first is authoritative and the second -- whether post-sync or
// fixed-schedule, whichever order they arrive in -- observes it under the
// lock and reuses its run instead of inserting a duplicate.
//
// Both orderings are exercised because the fix must not be directional: the
// original SkipIfCovered-only design only worked when post-sync happened to
// go first.
func TestStartRunTxDeduplicatesDoraAcrossGenerationsForTheSameDay(t *testing.T) {
	for _, order := range []struct {
		name             string
		firstGeneration  string
		firstScopeKey    string
		secondGeneration string
		secondScopeKey   string
	}{
		{
			name:             "post-sync first, fixed-schedule second",
			firstGeneration:  "post-sync:historical-sync-run-id",
			firstScopeKey:    "post-sync-scope-key",
			secondGeneration: "fixed-schedule:dora_daily_fanout:2026-08-24T02:15:00Z",
			secondScopeKey:   "2026-08-24",
		},
		{
			name:             "fixed-schedule first, post-sync second",
			firstGeneration:  "fixed-schedule:dora_daily_fanout:2026-08-24T02:15:00Z",
			firstScopeKey:    "2026-08-24",
			secondGeneration: "post-sync:historical-sync-run-id",
			secondScopeKey:   "post-sync-scope-key",
		},
	} {
		t.Run(order.name, func(t *testing.T) {
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
			const orgID = "00000000-0000-4000-8000-000000004242"
			scope := json.RawMessage(`{"version":1,"day":"2026-08-24","sink":"auto","interval":"daily","backfill_days":1}`)

			// First trigger: creates the real run and completes its partition
			// -- simulating the worker actually computing that day.
			firstTx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			firstRun, err := store.StartRunTx(ctx, firstTx, StartRunRequest{
				OrganizationID: orgID,
				Family:         "dora",
				Generation:     order.firstGeneration,
				ScopeKey:       order.firstScopeKey,
				Scopes:         []json.RawMessage{scope},
			}, nopPartitionPublisher{})
			if err != nil {
				t.Fatalf("first StartRunTx: %v", err)
			}
			if err := firstTx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			partitionID := deterministicPartitionID(firstRun.ID, 1)
			claim, err := store.ClaimPartition(ctx, partitionID)
			if err != nil {
				t.Fatal(err)
			}
			if claim == nil {
				t.Fatal("first run's partition was not claimable")
			}
			if err := store.CompletePartition(ctx, *claim, "rows=1;sha256=first"); err != nil {
				t.Fatal(err)
			}

			// Second trigger: DIFFERENT generation, SAME org+day. Must
			// return the FIRST run, not create a second one.
			secondTx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			secondRun, err := store.StartRunTx(ctx, secondTx, StartRunRequest{
				OrganizationID: orgID,
				Family:         "dora",
				Generation:     order.secondGeneration,
				ScopeKey:       order.secondScopeKey,
				Scopes:         []json.RawMessage{scope},
			}, nopPartitionPublisher{})
			if err != nil {
				t.Fatalf("second StartRunTx: %v", err)
			}
			if err := secondTx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if secondRun.ID != firstRun.ID {
				t.Fatalf(
					"the second trigger created a NEW run (%s) instead of reusing "+
						"the already-succeeded one (%s) -- this is the exact "+
						"duplicate-write shape codex round 1 demonstrated",
					secondRun.ID, firstRun.ID,
				)
			}

			var runCount int
			if err := pool.QueryRow(ctx,
				"SELECT count(*) FROM remaining_metric_runs WHERE org_id = $1::uuid AND family = 'dora'",
				orgID,
			).Scan(&runCount); err != nil {
				t.Fatal(err)
			}
			if runCount != 1 {
				t.Fatalf("expected exactly 1 dora run for this org+day, found %d", runCount)
			}
		})
	}
}

type nopPartitionPublisher struct{}

func (nopPartitionPublisher) PublishPartitionTx(
	context.Context, pgx.Tx, Run, Partition, string,
) error {
	return nil
}

// TestStartRunTxDoesNotLetANarrowRunSatisfyAWiderBackfillRequest is the
// codex-round-3 fix for a real gap in the round-2 dedup: loadRunCoveringDay
// matched on (org, family, day) alone. The fixed schedule always requests
// backfill_days=1; post-sync can request up to 90 for a real gap catch-up
// (postSyncRemainingScope, cmd/dev-health-worker/sync_dispatch.go). Before
// this fix, a backfill_days=1 run succeeding FIRST for an anchor day would
// satisfy a LATER backfill_days=30 request for that same anchor day,
// silently leaving the 29 days behind the anchor uncomputed while the wider
// request was told "already covered".
func TestStartRunTxDoesNotLetANarrowRunSatisfyAWiderBackfillRequest(t *testing.T) {
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
	const orgID = "00000000-0000-4000-8000-000000004242"

	// Narrow run: the fixed schedule's usual backfill_days=1, succeeds first.
	narrowScope := json.RawMessage(`{"version":1,"day":"2026-08-24","sink":"auto","interval":"daily","backfill_days":1}`)
	narrowTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	narrowRun, err := store.StartRunTx(ctx, narrowTx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "dora",
		Generation:     "fixed-schedule:dora_daily_fanout:2026-08-24T02:15:00Z",
		ScopeKey:       "2026-08-24",
		Scopes:         []json.RawMessage{narrowScope},
	}, nopPartitionPublisher{})
	if err != nil {
		t.Fatalf("narrow StartRunTx: %v", err)
	}
	if err := narrowTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	narrowPartitionID := deterministicPartitionID(narrowRun.ID, 1)
	claim, err := store.ClaimPartition(ctx, narrowPartitionID)
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil {
		t.Fatal("narrow run's partition was not claimable")
	}
	if err := store.CompletePartition(ctx, *claim, "rows=1;sha256=narrow"); err != nil {
		t.Fatal(err)
	}

	// Wide run: post-sync backfill catch-up, SAME anchor day, backfill_days=30.
	wideScope := json.RawMessage(`{"version":1,"day":"2026-08-24","sink":"auto","interval":"daily","backfill_days":30}`)
	wideTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wideRun, err := store.StartRunTx(ctx, wideTx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "dora",
		Generation:     "post-sync:catch-up-sync-run-id",
		ScopeKey:       "post-sync-wide-scope",
		Scopes:         []json.RawMessage{wideScope},
	}, nopPartitionPublisher{})
	if err != nil {
		t.Fatalf("wide StartRunTx: %v", err)
	}
	if err := wideTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	if wideRun.ID == narrowRun.ID {
		t.Fatalf(
			"the wider backfill_days=30 request was satisfied by the narrower "+
				"backfill_days=1 run (%s) -- this silently drops 29 days of "+
				"catch-up work behind the anchor day",
			narrowRun.ID,
		)
	}
	// The wide run must actually have a claimable partition -- it was not
	// itself silently skipped either.
	widePartition, err := store.ClaimPartition(ctx, deterministicPartitionID(wideRun.ID, 1))
	if err != nil {
		t.Fatal(err)
	}
	if widePartition == nil {
		t.Fatal("the wide run's own partition is not claimable")
	}
}

// TestStartRunTxDoesNotTreatACanceledRunAsCoverage is the other codex-round-3
// fix: the coverage query filtered partition.status='succeeded' but not
// run.status, so a partition marked succeeded whose run was (independently)
// canceled would still read back as valid coverage, and a later trigger
// would mint a completion fence for work the domain itself disowned.
func TestStartRunTxDoesNotTreatACanceledRunAsCoverage(t *testing.T) {
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
	const orgID = "00000000-0000-4000-8000-000000004242"
	scope := json.RawMessage(`{"version":1,"day":"2026-08-24","sink":"auto","interval":"daily","backfill_days":1}`)

	firstTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	firstRun, err := store.StartRunTx(ctx, firstTx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "dora",
		Generation:     "post-sync:sync-run-1",
		ScopeKey:       "post-sync-scope-1",
		Scopes:         []json.RawMessage{scope},
	}, nopPartitionPublisher{})
	if err != nil {
		t.Fatal(err)
	}
	if err := firstTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	partitionID := deterministicPartitionID(firstRun.ID, 1)
	claim, err := store.ClaimPartition(ctx, partitionID)
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil {
		t.Fatal("first run's partition was not claimable")
	}
	if err := store.CompletePartition(ctx, *claim, "rows=1;sha256=first"); err != nil {
		t.Fatal(err)
	}

	// Force the exact racy state codex described directly: the partition
	// stays 'succeeded' (CompletePartition's own write, untouched) but the
	// RUN independently transitions to canceled -- reproducing "CancelRun
	// changes the run to canceled" without needing to actually win a race
	// against CompletePartition's own internal transition.
	if _, err := pool.Exec(ctx,
		"UPDATE remaining_metric_runs SET status = 'canceled' WHERE id = $1::uuid",
		firstRun.ID,
	); err != nil {
		t.Fatal(err)
	}

	secondTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	secondRun, err := store.StartRunTx(ctx, secondTx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "dora",
		Generation:     "fixed-schedule:dora_daily_fanout:2026-08-24T02:15:00Z",
		ScopeKey:       "2026-08-24",
		Scopes:         []json.RawMessage{scope},
	}, nopPartitionPublisher{})
	if err != nil {
		t.Fatalf("second StartRunTx: %v", err)
	}
	if err := secondTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	if secondRun.ID == firstRun.ID {
		t.Fatalf(
			"a canceled run (%s) was returned as coverage -- a completion "+
				"fence was minted for work the domain disowned",
			firstRun.ID,
		)
	}
}
