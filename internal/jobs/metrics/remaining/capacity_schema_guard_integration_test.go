//go:build integration

package remaining

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestCapacityRefusesAStaleSchema is the regression test for the gap that
// registering on any reachable connection left open.
//
// A worker whose database is reachable but stale used to register the capacity
// kind, claim partitions, fail the query or the insert, and retry -- burning
// attempts while capacity stayed unavailable and while the outward signal was a
// counter that never moved. Refusing at construction converts the same drift
// into an unclaimed backlog plus one loud, reason-labelled refusal.
//
// The stale state is built by DROPPING a column the executor actually uses,
// rather than by pointing at an empty database: a missing table is the easy
// case, and the failure that matters is a table that exists and looks fine
// until the one column this code needs turns out to be absent.
func TestCapacityRefusesAStaleSchema(t *testing.T) {
	ctx := context.Background()
	conn := migratedClickHouse(t, ctx, OperationalOrderingRevision)

	// Baseline: the real schema must BUILD, or the assertions below would pass
	// for a guard that refuses everything.
	if _, err := NewCapacityExecutor(ctx, conn, nil); err != nil {
		t.Fatalf("the real migrated schema must be accepted: %v", err)
	}

	t.Run("a missing column on a read table is refused", func(t *testing.T) {
		fresh := freshMigratedClickHouse(t, ctx, OperationalOrderingRevision)
		if err := fresh.Exec(ctx,
			"ALTER TABLE work_item_metrics_daily DROP COLUMN wip_count_end_of_day",
		); err != nil {
			t.Fatalf("stage the stale schema: %v", err)
		}
		_, err := NewCapacityExecutor(ctx, fresh, nil)
		if err == nil {
			t.Fatal(
				"a database missing a column the backlog query reads was accepted; " +
					"the handler would claim partitions and fail every one")
		}
		if !errors.Is(err, ErrCapacitySchemaIncompatible) {
			t.Fatalf("expected a schema-incompatible refusal, got: %v", err)
		}
		// The operator has to know WHICH column, or the refusal says only that
		// something is wrong.
		if !strings.Contains(err.Error(), "wip_count_end_of_day") {
			t.Errorf("the refusal must name the missing column: %v", err)
		}
	})

	t.Run("a read table on the wrong engine is refused", func(t *testing.T) {
		// The column half of the probe passes here by construction: this table
		// is rebuilt with EVERY required column present and only its ENGINE
		// changed. That is the whole point -- a column-only probe accepted this
		// database, and then the executor's `FINAL` reads silently became
		// no-ops, so superseded rows stayed visible and aggregated into the
		// forecast. Wrong numbers, reported as a successful run.
		fresh := freshMigratedClickHouse(t, ctx, OperationalOrderingRevision)
		if err := fresh.Exec(ctx, "DROP TABLE work_item_metrics_daily"); err != nil {
			t.Fatalf("stage the wrong-engine schema: %v", err)
		}
		// Columns and types mirror the migrated table for the six the executor
		// names; the ORDER BY key matches what a ReplacingMergeTree would use,
		// so the ONLY difference from an acceptable deployment is the engine.
		if err := fresh.Exec(ctx, `
            CREATE TABLE work_item_metrics_daily (
                day Date,
                org_id String,
                team_id LowCardinality(String),
                work_scope_id LowCardinality(String),
                items_completed UInt32,
                wip_count_end_of_day UInt32,
                computed_at DateTime64(3)
            ) ENGINE = MergeTree
            ORDER BY (org_id, day, work_scope_id, team_id)
        `); err != nil {
			t.Fatalf("create the plain-MergeTree table: %v", err)
		}

		// Guard the guard: if this staging ever stopped producing a table with
		// all six columns, the refusal below would fire for the COLUMN reason
		// and this test would pass while proving nothing about the engine.
		present, err := capacityTableColumns(ctx, fresh, "work_item_metrics_daily")
		if err != nil {
			t.Fatalf("inspect the staged table: %v", err)
		}
		for _, column := range capacityTableRequirements["work_item_metrics_daily"].columns {
			if !present[column] {
				t.Fatalf(
					"the staged table is missing %q, so a refusal here would be "+
						"about columns and this test would not exercise the engine "+
						"check at all", column)
			}
		}

		_, err = NewCapacityExecutor(ctx, fresh, nil)
		if err == nil {
			t.Fatal(
				"a plain MergeTree was accepted for a table this code reads with " +
					"FINAL; duplicate versions would aggregate into the forecast " +
					"and be written as a successful result")
		}
		if !errors.Is(err, ErrCapacitySchemaIncompatible) {
			t.Fatalf("expected a schema-incompatible refusal, got: %v", err)
		}
		if !strings.Contains(err.Error(), "work_item_metrics_daily") {
			t.Errorf("the refusal must name the offending table: %v", err)
		}
		// The engine actually found, not just the one required: an operator
		// reading this needs to know what is deployed, not only what is wanted.
		if !strings.Contains(err.Error(), "MergeTree") {
			t.Errorf("the refusal must name the engine it found: %v", err)
		}
		// And it must not be mistaken for the column failure mode.
		if strings.Contains(err.Error(), "is missing") {
			t.Errorf(
				"the refusal reads as a missing-column failure, which would send "+
					"an operator to the wrong half of the migration: %v", err)
		}
	})

	t.Run("a missing write table is refused", func(t *testing.T) {
		fresh := freshMigratedClickHouse(t, ctx, OperationalOrderingRevision)
		if err := fresh.Exec(ctx, "DROP TABLE capacity_forecasts"); err != nil {
			t.Fatalf("stage the stale schema: %v", err)
		}
		_, err := NewCapacityExecutor(ctx, fresh, nil)
		if err == nil {
			t.Fatal("a database with no capacity_forecasts table was accepted")
		}
		if !errors.Is(err, ErrCapacitySchemaIncompatible) {
			t.Fatalf("expected a schema-incompatible refusal, got: %v", err)
		}
		if !strings.Contains(err.Error(), "capacity_forecasts") {
			t.Errorf("the refusal must name the missing table: %v", err)
		}
	})
}
