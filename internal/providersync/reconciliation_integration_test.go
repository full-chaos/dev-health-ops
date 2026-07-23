//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestFreshProcessReconcilesCrashAfterClickHouseWriteFromPostgresPayload(
	t *testing.T,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := postgres.Close(closeContext); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := clickhouseInstance.Close(closeContext); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	}()

	pool, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_units
SET dataset_key = 'repo-metadata', cost_class = 'light', processor_flags = '{}'
WHERE id = $1`, firstUnitID); err != nil {
		t.Fatal(err)
	}
	conn, err := clickhousestore.Open(
		ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Exec(ctx, `
CREATE TABLE provider_records (
	schema_version String,
	provider String,
	org_id String,
	integration_id String,
	entity_type String,
	source_id String,
	dedupe_key String,
	observed_at DateTime64(9, 'UTC'),
	provenance_source String,
	provenance_confidence String,
	provenance_evidence_id String,
	attributes_json String
) ENGINE = MergeTree
ORDER BY (org_id, entity_type, dedupe_key)
SETTINGS non_replicated_deduplication_window = 100`); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	firstRepository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	firstClaim, err := firstRepository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := providerfoundation.BuildGenerationBlocks(
		firstClaim.GenerationKey(),
		"provider_records",
		[]providerfoundation.NormalizedEnvelope{{
			SchemaVersion: "v1", Provider: "github", OrgID: "org-acme",
			IntegrationID: firstIntegrationID, EntityType: "repository",
			SourceID:   "github:repo:acme/api",
			DedupeKey:  "github:repository:github:repo:acme/api",
			ObservedAt: now,
			Provenance: providerfoundation.Provenance{
				Source: "github_rest", Confidence: "1.0",
				EvidenceID: "github:repo-metadata:" + firstUnitID,
			},
			Attributes: map[string]string{
				"name": "acme/api", "default_branch": "main",
			},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := NewGenerationJournalState(blocks, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := firstRepository.Prepare(
		ctx, firstClaim, journal, now.Add(time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if err := firstRepository.BeginBlock(
		ctx, firstClaim, 0, blocks[0].ContentDigest(), now.Add(2*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	firstSink := providerfoundation.ClickHouseSink{
		Conn: conn, Table: "provider_records",
		Lease: providerfoundation.LeaseGuardFunc(func(ctx context.Context) error {
			return firstRepository.Assert(ctx, firstClaim, now.Add(2*time.Second))
		}),
		ReplayGuard: providerfoundation.NewGenerationReplayGuard(),
	}
	if err := firstSink.WriteGenerationBlock(ctx, blocks[0]); err != nil {
		t.Fatal(err)
	}
	// Simulate process death in the kill window: the ClickHouse write is
	// durable, the Postgres block remains writing, and no block object survives.
	blocks = nil
	journal = GenerationJournalState{}
	firstRepository = nil

	freshRepository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	recoveryNow := now.Add(61 * time.Second)
	recoveredClaim, err := freshRepository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, Owner: uuid.NewString(), Now: recoveryNow,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	freshReadback := providerfoundation.ClickHouseSink{
		Conn: conn, Table: "provider_records",
		Lease: providerfoundation.LeaseGuardFunc(func(ctx context.Context) error {
			return freshRepository.Assert(ctx, recoveredClaim, recoveryNow)
		}),
	}
	result, err := (OperatorReconciler{
		Journal: freshRepository, Readback: freshReadback,
		Now: func() time.Time { return recoveryNow },
	}).Reconcile(ctx, recoveredClaim)
	if err != nil || result.MarkedCommitted != 1 ||
		result.ResetPending != 0 || result.AlreadySafe != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	recoveredState, err := freshRepository.Load(
		ctx, recoveredClaim, recoveryNow.Add(time.Second),
	)
	if err != nil || recoveredState.Blocks[0].Status != GenerationBlockCommitted ||
		len(recoveredState.Blocks[0].RecoveryPayload) != 1 {
		t.Fatalf("state=%+v error=%v", recoveredState, err)
	}
}
