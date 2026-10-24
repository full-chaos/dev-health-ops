package sync

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeUnitRow struct {
	id, org, runID, integrationID, sourceID, provider, dataset, cost, mode string
	since, before                                                          *time.Time
	status                                                                 string
	flags                                                                  []byte
	createdAt                                                              time.Time
}

type unitTxStats struct {
	batchSizes, ledgerLens, verifyIDs, verifyRows []int
	counts                                        int
}

func (stats unitTxStats) maxOf(values []int) int {
	most := 0
	for _, value := range values {
		most = max(most, value)
	}
	return most
}

type fakeUnitTx struct {
	table map[string]*fakeUnitRow
	stats unitTxStats
}

func newFakeUnitTx() *fakeUnitTx { return &fakeUnitTx{table: map[string]*fakeUnitRow{}} }

func (tx *fakeUnitTx) Exec(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
	tx.stats.ledgerLens = append(tx.stats.ledgerLens, len(args[0].([]string)))
	return pgconn.CommandTag{}, nil
}

func (tx *fakeUnitTx) SendBatch(_ context.Context, batch *pgx.Batch) pgx.BatchResults {
	tx.stats.batchSizes = append(tx.stats.batchSizes, batch.Len())
	for _, queued := range batch.QueuedQueries {
		args := queued.Arguments
		id := args[0].(string)
		if _, exists := tx.table[id]; exists {
			continue
		}
		tx.table[id] = &fakeUnitRow{
			id: id, org: args[1].(string), runID: args[2].(string), integrationID: args[3].(string),
			sourceID: args[4].(string), provider: args[5].(string), dataset: args[6].(string),
			cost: args[7].(string), mode: args[8].(string), since: args[9].(*time.Time),
			before: args[10].(*time.Time), status: "planned", flags: args[11].([]byte),
			createdAt: args[12].(time.Time),
		}
	}
	return &fakeBatchResults{remaining: batch.Len()}
}

type fakeBatchResults struct {
	pgx.BatchResults
	remaining int
}

func (results *fakeBatchResults) Exec() (pgconn.CommandTag, error) {
	results.remaining--
	return pgconn.CommandTag{}, nil
}
func (results *fakeBatchResults) Close() error { return nil }

func (tx *fakeUnitTx) Query(_ context.Context, _ string, args ...any) (pgx.Rows, error) {
	wanted := args[1].([]string)
	tx.stats.verifyIDs = append(tx.stats.verifyIDs, len(wanted))
	sort.Strings(wanted)
	var rows []*fakeUnitRow
	for _, id := range wanted {
		if row, present := tx.table[id]; present {
			rows = append(rows, row)
		}
	}
	tx.stats.verifyRows = append(tx.stats.verifyRows, len(rows))
	return &fakeUnitRows{rows: rows, index: -1}, nil
}

func (tx *fakeUnitTx) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	tx.stats.counts++
	return fakeCountRow{count: len(tx.table)}
}

type fakeCountRow struct{ count int }

func (row fakeCountRow) Scan(dest ...any) error {
	*(dest[0].(*int)) = row.count
	return nil
}

type fakeUnitRows struct {
	pgx.Rows
	rows  []*fakeUnitRow
	index int
}

func (rows *fakeUnitRows) Next() bool { rows.index++; return rows.index < len(rows.rows) }
func (rows *fakeUnitRows) Close()     {}
func (rows *fakeUnitRows) Err() error { return nil }
func (rows *fakeUnitRows) Scan(dest ...any) error {
	row := rows.rows[rows.index]
	values := []any{
		row.id, row.org, row.runID, row.integrationID, row.sourceID, row.provider, row.dataset,
		row.cost, row.mode, row.since, row.before, row.status, 0, (*time.Time)(nil), 0,
		(*time.Time)(nil), 0, (*string)(nil), (*time.Time)(nil), (*int)(nil), (*string)(nil),
		[]byte(nil), row.flags, (*string)(nil), (*time.Time)(nil), (*time.Time)(nil),
		row.createdAt, row.createdAt,
	}
	if len(dest) != len(values) {
		return fmt.Errorf("scan got %d destinations, want %d", len(dest), len(values))
	}
	for index, value := range values {
		reflect.ValueOf(dest[index]).Elem().Set(reflect.ValueOf(value))
	}
	return nil
}

func chunkTestUnits(count int) []PlannedUnit {
	since := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	before := since.Add(7 * 24 * time.Hour)
	units := make([]PlannedUnit, count)
	for index := range units {
		units[index] = PlannedUnit{
			OrgID: "org", IntegrationID: "00000000-0000-4000-8000-000000000001",
			SourceID: fmt.Sprintf("00000000-0000-4000-8000-%012d", index%97), Provider: "jira",
			Dataset: []string{"work-items", "work-item-history"}[index%2], CostClass: "rest_core",
			Mode: "backfill", WindowStart: &since, WindowEnd: &before,
			ProcessorFlags: map[string]bool{"sync_work_items": true},
		}
	}
	return units
}

func chunkTestIDs(t *testing.T) materializationIDs {
	t.Helper()
	ids, err := deterministicMaterializationIDs("occurrence-v1:chunks")
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

var chunkTestCreatedAt = time.Date(2026, 10, 24, 1, 0, 0, 0, time.UTC)

func TestPersistPlannedUnitsHoldsEveryStatementToOneChunk(t *testing.T) {
	const size = unitChunkSize
	units := chunkTestUnits(10*size + 7)
	ids := chunkTestIDs(t)
	chunked := newFakeUnitTx()
	if err := persistPlannedUnits(context.Background(), chunked, ids, units, chunkTestCreatedAt, size); err != nil {
		t.Fatal(err)
	}
	stats := chunked.stats
	if len(stats.batchSizes) != 11 || len(stats.ledgerLens) != 11 || len(stats.verifyIDs) != 11 || stats.counts != 1 {
		t.Fatalf("batches=%d ledgers=%d verifies=%d counts=%d, want 11/11/11/1", len(stats.batchSizes), len(stats.ledgerLens), len(stats.verifyIDs), stats.counts)
	}
	for name, values := range map[string][]int{"batch": stats.batchSizes, "ledger array": stats.ledgerLens, "verify ids": stats.verifyIDs, "verify rows": stats.verifyRows} {
		if got := stats.maxOf(values); got > size {
			t.Fatalf("largest %s holds %d, bound is %d", name, got, size)
		}
	}
	if len(chunked.table) != len(units) {
		t.Fatalf("persisted %d units, want %d", len(chunked.table), len(units))
	}
	for ordinal, unit := range units {
		id, err := deterministicUnitID(ids.SyncRunID, ordinal)
		if err != nil {
			t.Fatal(err)
		}
		row := chunked.table[id]
		if row == nil || row.provider != unit.Provider || row.dataset != unit.Dataset || row.sourceID != unit.SourceID || row.status != "planned" {
			t.Fatalf("unit %d persisted as %+v, want %+v", ordinal, row, unit)
		}
	}

	// The unchunked form (one chunk of every unit) persists the same table, and
	// it is the one the bound check refuses: the check bites.
	whole := newFakeUnitTx()
	if err := persistPlannedUnits(context.Background(), whole, ids, units, chunkTestCreatedAt, len(units)); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(whole.table, chunked.table) {
		t.Fatal("chunked and unchunked runs persisted different tables")
	}
	if got := whole.stats.maxOf(whole.stats.batchSizes); got <= size {
		t.Fatalf("the unchunked batch holds %d, want more than the bound %d", got, size)
	}
	if got := whole.stats.maxOf(whole.stats.ledgerLens); got <= size {
		t.Fatalf("the unchunked ledger array holds %d, want more than the bound %d", got, size)
	}
	if got := whole.stats.maxOf(whole.stats.verifyRows); got <= size {
		t.Fatalf("the unchunked verify read holds %d rows, want more than the bound %d", got, size)
	}
}

func TestPersistPlannedUnitsReplaysToTheSameRows(t *testing.T) {
	units := chunkTestUnits(2*unitChunkSize + 3)
	ids := chunkTestIDs(t)
	tx := newFakeUnitTx()
	for pass := 0; pass < 2; pass++ {
		if err := persistPlannedUnits(context.Background(), tx, ids, units, chunkTestCreatedAt, unitChunkSize); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
	}
	if len(tx.table) != len(units) {
		t.Fatalf("a replay left %d rows, want %d", len(tx.table), len(units))
	}
}

func TestPersistPlannedUnitsRefusesDriftAndExtras(t *testing.T) {
	units := chunkTestUnits(unitChunkSize + 5)
	ids := chunkTestIDs(t)
	tx := newFakeUnitTx()
	if err := persistPlannedUnits(context.Background(), tx, ids, units, chunkTestCreatedAt, unitChunkSize); err != nil {
		t.Fatal(err)
	}
	lastID, err := deterministicUnitID(ids.SyncRunID, len(units)-1)
	if err != nil {
		t.Fatal(err)
	}
	tx.table[lastID].status = "running"
	err = persistPlannedUnits(context.Background(), tx, ids, units, chunkTestCreatedAt, unitChunkSize)
	if !errors.Is(err, ErrInvalidPlan) || !strings.Contains(err.Error(), "maps to different state") {
		t.Fatalf("a drifted row: err = %v, want ErrInvalidPlan maps to different state", err)
	}
	tx.table[lastID].status = "planned"
	tx.table["00000000-0000-4000-8000-00000000ffff"] = &fakeUnitRow{id: "00000000-0000-4000-8000-00000000ffff", status: "planned"}
	err = persistPlannedUnits(context.Background(), tx, ids, units, chunkTestCreatedAt, unitChunkSize)
	if !errors.Is(err, ErrInvalidPlan) || !strings.Contains(err.Error(), "has") {
		t.Fatalf("an extra row: err = %v, want ErrInvalidPlan with the unit count", err)
	}
	if err := persistPlannedUnits(context.Background(), newFakeUnitTx(), ids, units, chunkTestCreatedAt, 0); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("chunk size 0: err = %v, want ErrInvalidPlan", err)
	}
}
