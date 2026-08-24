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
// fix that both native executors depend on identically. No throughput
// history is seeded, so the executor legitimately skips every scope and
// writes nothing -- the assertion that matters is that it gets PAST the
// scope decode at all (before the fix this failed with the same "durable
// state is invalid ... scope: unexpected end of JSON input" as DORA did).
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
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	scope := json.RawMessage(`{"version":1,"all_teams":true,"history_days":90,"simulations":100}`)
	run, err := store.StartRun(ctx, StartRunRequest{
		OrganizationID: orgID,
		Family:         "capacity",
		Generation:     "capacity-v1",
		ScopeKey:       "all-teams",
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
}
