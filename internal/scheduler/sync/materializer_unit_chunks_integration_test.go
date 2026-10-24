//go:build integration

package sync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type countingUnitTx struct {
	pgx.Tx
	batchSizes []int
	verifyRows []int
}

func (tx *countingUnitTx) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	tx.batchSizes = append(tx.batchSizes, batch.Len())
	return tx.Tx.SendBatch(ctx, batch)
}

func (tx *countingUnitTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows, err := tx.Tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &countingUnitRows{Rows: rows, owner: tx}, nil
}

type countingUnitRows struct {
	pgx.Rows
	owner  *countingUnitTx
	count  int
	closed bool
}

func (rows *countingUnitRows) Next() bool {
	more := rows.Rows.Next()
	if more {
		rows.count++
	}
	return more
}

func (rows *countingUnitRows) Close() {
	if !rows.closed {
		rows.closed = true
		rows.owner.verifyRows = append(rows.owner.verifyRows, rows.count)
	}
	rows.Rows.Close()
}

// The statements run against Postgres, not a double: a plan of 1200 units is
// materialized, then the same units are persisted again through a counting
// transaction. The replay must pass its own verify (the SQL, including the id
// array read), and no batch or verify read may hold more than the chunk size.
func TestPersistPlannedUnitsChunksAgainstPostgres(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	since := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	occurrence := startBackfillTriggerFixture(t, fixture, since, before)
	ctx := context.Background()
	const jiraSourceID = "00000000-0000-4000-8000-000000002003"
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO integration_sources (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
SELECT gen_random_uuid(),org_id,integration_id,provider,source_type,'PROJ'||n,'PROJ'||n,'PROJ'||n,TRUE,metadata,now(),now()
FROM public.integration_sources, generate_series(1, 399) AS n WHERE id = $1::uuid`, jiraSourceID); err != nil {
		t.Fatal(err)
	}
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := materializeAndCommit(t, fixture, materializer, occurrence)
	if err != nil {
		t.Fatal(err)
	}
	if plan.PlannedUnits <= unitChunkSize {
		t.Fatalf("planned %d units, want more than one chunk of %d", plan.PlannedUnits, unitChunkSize)
	}
	ids, err := deterministicMaterializationIDs(occurrence.ID)
	if err != nil {
		t.Fatal(err)
	}

	stored := map[string]PlannedUnit{}
	rows, err := fixture.pool.Query(ctx, `SELECT id::text,org_id,integration_id::text,source_id::text,provider,dataset_key,cost_class,mode,since_at,before_at,processor_flags::jsonb FROM public.sync_run_units WHERE sync_run_id=$1::uuid`, ids.SyncRunID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		var unit PlannedUnit
		var flagsJSON []byte
		if err := rows.Scan(&id, &unit.OrgID, &unit.IntegrationID, &unit.SourceID, &unit.Provider, &unit.Dataset, &unit.CostClass, &unit.Mode, &unit.WindowStart, &unit.WindowEnd, &flagsJSON); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(flagsJSON, &unit.ProcessorFlags); err != nil {
			t.Fatal(err)
		}
		stored[id] = unit
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(stored) != plan.PlannedUnits {
		t.Fatalf("stored %d units, planned %d", len(stored), plan.PlannedUnits)
	}
	units := make([]PlannedUnit, plan.PlannedUnits)
	for ordinal := range units {
		id, err := deterministicUnitID(ids.SyncRunID, ordinal)
		if err != nil {
			t.Fatal(err)
		}
		unit, present := stored[id]
		if !present {
			t.Fatalf("unit %d (%s) is not stored", ordinal, id)
		}
		units[ordinal] = unit
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	counting := &countingUnitTx{Tx: tx}
	if err := persistPlannedUnits(ctx, counting, ids, units, occurrence.ScheduledFor, unitChunkSize); err != nil {
		t.Fatalf("a replay of the same plan failed its own verify: %v", err)
	}
	wantChunks := (len(units) + unitChunkSize - 1) / unitChunkSize
	if len(counting.batchSizes) != wantChunks || len(counting.verifyRows) != wantChunks {
		t.Fatalf("batches=%d verifies=%d, want %d of each", len(counting.batchSizes), len(counting.verifyRows), wantChunks)
	}
	for _, size := range counting.batchSizes {
		if size > unitChunkSize {
			t.Fatalf("a batch held %d units, bound %d", size, unitChunkSize)
		}
	}
	for _, size := range counting.verifyRows {
		if size > unitChunkSize {
			t.Fatalf("a verify read returned %d rows, bound %d", size, unitChunkSize)
		}
	}
}
