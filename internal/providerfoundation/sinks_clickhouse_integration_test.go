//go:build integration

package providerfoundation

import (
	"context"
	"errors"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestClickHouseGenerationBlocksDeduplicateRetriesAndRetainDistinctBlocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	}()
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Exec(ctx, generationTableDDL("provider_records", true)); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, generationTableDDL("unsafe_provider_records", false)); err != nil {
		t.Fatal(err)
	}

	blocks, err := buildGenerationBlocks(
		"sync-unit:11111111-1111-4111-8111-111111111111",
		"provider_records",
		[]NormalizedEnvelope{testGenerationEnvelope("record-2"), testGenerationEnvelope("record-1")},
		1,
		maxGenerationBlockBytes,
	)
	if err != nil || len(blocks) != 2 {
		t.Fatalf("blocks=%d error=%v", len(blocks), err)
	}
	sink := ClickHouseSink{
		Conn: conn, Table: "provider_records",
		Lease: LeaseGuardFunc(func(context.Context) error {
			return nil
		}),
		ReplayGuard: NewGenerationReplayGuard(),
	}
	if inspection, err := sink.InspectGenerationBlock(ctx, blocks[0]); err != nil ||
		inspection != GenerationBlockAbsent {
		t.Fatalf("pre-write inspection=%q error=%v", inspection, err)
	}
	if err := sink.WriteGenerationBlock(ctx, blocks[0]); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectGenerationBlock(ctx, blocks[0]); err != nil ||
		inspection != GenerationBlockExact {
		t.Fatalf("post-write inspection=%q error=%v", inspection, err)
	}
	if err := sink.WriteGenerationBlock(ctx, blocks[0]); err != nil {
		t.Fatalf("identical block retry failed: %v", err)
	}
	if err := sink.WriteGenerationBlock(ctx, blocks[1]); err != nil {
		t.Fatal(err)
	}
	var rows, uniqueRows uint64
	if err := conn.QueryRow(ctx, "SELECT count(), uniqExact(dedupe_key) FROM provider_records").Scan(&rows, &uniqueRows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 || uniqueRows != 2 {
		t.Fatalf("rows=%d unique_rows=%d", rows, uniqueRows)
	}
	conflicting := blocks[0].Batch()[0]
	conflicting.Attributes = map[string]string{"name": "conflicting-version"}
	if err := sink.WriteBatch(ctx, []NormalizedEnvelope{conflicting}); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectGenerationBlock(ctx, blocks[0]); err != nil ||
		inspection != GenerationBlockConflict {
		t.Fatalf("exact plus conflict inspection=%q error=%v", inspection, err)
	}

	partialBlocks, err := BuildGenerationBlocks(
		"sync-unit:33333333-3333-4333-8333-333333333333",
		"provider_records",
		[]NormalizedEnvelope{
			testGenerationEnvelope("partial-a"),
			testGenerationEnvelope("partial-b"),
		},
	)
	if err != nil || len(partialBlocks) != 1 {
		t.Fatalf("partial blocks=%d error=%v", len(partialBlocks), err)
	}
	if err := sink.WriteBatch(ctx, partialBlocks[0].Batch()[:1]); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectGenerationBlock(ctx, partialBlocks[0]); err != nil ||
		inspection != GenerationBlockConflict {
		t.Fatalf("partial inspection=%q error=%v", inspection, err)
	}

	unsafeBlocks, err := buildGenerationBlocks(
		"sync-unit:22222222-2222-4222-8222-222222222222",
		"unsafe_provider_records",
		[]NormalizedEnvelope{testGenerationEnvelope("unsafe")},
		1,
		maxGenerationBlockBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	unsafeSink := ClickHouseSink{
		Conn: conn, Table: "unsafe_provider_records",
		Lease: LeaseGuardFunc(func(context.Context) error {
			return nil
		}),
		ReplayGuard: NewGenerationReplayGuard(),
	}
	if err := unsafeSink.WriteGenerationBlock(ctx, unsafeBlocks[0]); !errors.Is(err, ErrSinkGenerationUnsafe) {
		t.Fatalf("unsafe non-replicated table error=%v", err)
	}
}

func generationTableDDL(table string, dedupe bool) string {
	statement := `
CREATE TABLE ` + table + ` (
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
) ENGINE = MergeTree ORDER BY (org_id, entity_type, dedupe_key)`
	if dedupe {
		statement += " SETTINGS non_replicated_deduplication_window = 100"
	}
	return statement
}
