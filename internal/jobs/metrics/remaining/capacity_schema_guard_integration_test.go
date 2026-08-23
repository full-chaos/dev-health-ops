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
