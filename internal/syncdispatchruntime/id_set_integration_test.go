//go:build integration

package syncdispatchruntime

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func guardTempTables(t *testing.T, ctx context.Context, tx pgx.Tx) []string {
	t.Helper()
	rows, err := tx.Query(ctx, `SELECT relname FROM pg_class WHERE relpersistence = 't' AND relname LIKE 'dispatch_guard_%' ORDER BY relname`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	return names
}

// The guard sets live in memory up to unitPageSize ids and in a temp table after: the claim must take the same ids on
// both sides of the boundary, the table must not exist at N=K, and a second dispatch transaction on the same connection
// must work over the dropped and recreated temp table.
func TestDispatchGuardSetsSpillOnlyPastPageSizeAndTheClaimTakesTheSameIDs(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		const units = 1007
		pgseed.TierLimit(ctx, t, pool, "community", "max_sync_units", "2000")
		now := pgNow()
		ids := manyUnitIDs(units)
		for _, id := range ids {
			insertGuardUnit(t, ctx, pool, id, "github", "standard", syncRunUnitStatusPlanned, now, nil, nil, nil)
		}
		slots := concurrencyCapForCostClass("standard")
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()

		dispatchPass := func(t *testing.T) (claimed []string, tables []string) {
			t.Helper()
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			decision, err := authorizeRun(ctx, tx, nil, guardTestOrg, guardTestRun, now)
			if err != nil {
				t.Fatalf("authorizeRun: %v", err)
			}
			got, deferred, err := claimUnits(ctx, tx, guardTestRun, decision.authorized, decision.capped, now.Add(1))
			if err != nil {
				t.Fatalf("claimUnits: %v", err)
			}
			if len(deferred) != 0 {
				t.Fatalf("claimable outside the snapshot = %v, want none", deferred)
			}
			tables = guardTempTables(t, ctx, tx)
			for _, unit := range got {
				claimed = append(claimed, unit.id)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			return claimed, tables
		}

		first, tables := dispatchPass(t)
		if strings.Join(first, ",") != strings.Join(ids[:slots], ",") {
			t.Fatalf("claimed = %v, want the first %d ids by id", first, slots)
		}
		if got := strings.Join(tables, ","); got != "dispatch_guard_authorized,dispatch_guard_capped" {
			t.Fatalf("temp tables in the transaction = %q, want both sets spilled at N=%d", got, units)
		}
		// The tables are gone after the commit, and the second pass on the same connection (statements prepared over
		// the first pass's tables) finds the bucket full: it claims nothing and raises nothing.
		second, tables := dispatchPass(t)
		if len(second) != 0 {
			t.Fatalf("second pass claimed %v, want none (the bucket is at its cap)", second)
		}
		if len(tables) == 0 {
			t.Fatalf("second pass created no temp table for %d candidates", units-slots)
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if left := guardTempTables(t, ctx, tx); len(left) != 0 {
			t.Fatalf("temp tables after commit = %v, want none", left)
		}
	})
}

// At exactly unitPageSize candidates the authorized set is still in memory: no temp table is created; one more and it spills.
func TestDispatchGuardAuthorizedSetSpillsAtPageSizePlusOne(t *testing.T) {
	for _, units := range []int{unitPageSize, unitPageSize + 1} {
		t.Run(fmt.Sprintf("N=%d", units), func(t *testing.T) {
			withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
				now := pgNow()
				ids := manyUnitIDs(units)
				for _, id := range ids {
					insertGuardUnit(t, ctx, pool, id, "github", "standard", syncRunUnitStatusPlanned, now, nil, nil, nil)
				}
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback(ctx) }()
				decision, err := authorizeRun(ctx, tx, nil, guardTestOrg, guardTestRun, now)
				if err != nil {
					t.Fatalf("authorizeRun: %v", err)
				}
				claimed, _, err := claimUnits(ctx, tx, guardTestRun, decision.authorized, decision.capped, now.Add(1))
				if err != nil {
					t.Fatalf("claimUnits: %v", err)
				}
				slots := concurrencyCapForCostClass("standard")
				if len(claimed) != slots || claimed[0].id != ids[0] || claimed[slots-1].id != ids[slots-1] {
					t.Fatalf("claimed %d units, want the first %d by id", len(claimed), slots)
				}
				tables := guardTempTables(t, ctx, tx)
				want := ""
				if units > unitPageSize {
					want = "dispatch_guard_authorized"
				}
				if got := strings.Join(tables, ","); got != want {
					t.Fatalf("temp tables = %q, want %q at N=%d (authorized %d, capped %d)", got, want, units, units, units-slots)
				}
			})
		})
	}
}
