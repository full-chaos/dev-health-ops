//go:build integration

package syncdispatchruntime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pagedRowsTx records the number of rows every paged guard read returned.
type pagedRowsTx struct {
	pgx.Tx
	pages *[]int
}

func (tx pagedRowsTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows, err := tx.Tx.Query(ctx, sql, args...)
	if err != nil || !strings.Contains(sql, "FROM public.sync_run_units") || !strings.Contains(sql, "LIMIT") {
		return rows, err
	}
	return &countingRows{Rows: rows, pages: tx.pages}, nil
}

type countingRows struct {
	pgx.Rows
	seen   int
	pages  *[]int
	closed bool
}

func (rows *countingRows) Next() bool {
	more := rows.Rows.Next()
	if more {
		rows.seen++
	}
	return more
}

func (rows *countingRows) Close() {
	if !rows.closed {
		rows.closed = true
		*rows.pages = append(*rows.pages, rows.seen)
	}
	rows.Rows.Close()
}

func manyUnitIDs(n int) []string {
	ids := make([]string, n)
	for index := range ids {
		ids[index] = fmt.Sprintf("00000000-0000-4000-8000-%012x", 0x1000+index)
	}
	return ids
}

// A run with more than one page of units: every keyset read returns at most unitPageSize rows, each unit is read
// once per pass, and the decision claims exactly the first free slots by id.
func TestAuthorizeRunPagesAPastPageSizeRunAndKeepsTheDecision(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		const units = 1007
		now := pgNow()
		ids := manyUnitIDs(units)
		for _, id := range ids {
			insertGuardUnit(t, ctx, pool, id, "github", "standard", syncRunUnitStatusPlanned, now, nil, nil, nil)
		}
		run := func(t *testing.T, limit string) (guardDecision, []int) {
			t.Helper()
			t.Setenv("SYNC_RUN_MAX_UNITS", limit)
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			var pages []int
			decision, err := authorizeRun(ctx, pagedRowsTx{Tx: tx, pages: &pages}, nil, guardTestOrg, guardTestRun, now)
			if err != nil {
				t.Fatalf("authorizeRun: %v", err)
			}
			for _, size := range pages {
				if size > unitPageSize {
					t.Fatalf("a keyset read returned %d rows, want at most %d (pages %v)", size, unitPageSize, pages)
				}
			}
			return decision, pages
		}
		sum := func(pages []int) int {
			total := 0
			for _, size := range pages {
				total += size
			}
			return total
		}

		t.Run("under the total cap", func(t *testing.T) {
			decision, pages := run(t, "2000")
			if !decision.allowed || !decision.concurrencyCapped {
				t.Fatalf("decision = %+v, want an allowed, concurrency-capped decision", decision)
			}
			if got, want := sum(pages), 2*units; got != want {
				t.Fatalf("rows read = %d over pages %v, want %d (every unit once per pass)", got, pages, want)
			}
			slots := concurrencyCapForCostClass("standard")
			var claimable []string
			for _, id := range ids {
				if decision.authorizedUnitIDs[id] && !containsID(decision.cappedUnitIDs, id) {
					claimable = append(claimable, id)
				}
			}
			sort.Strings(claimable)
			if strings.Join(claimable, ",") != strings.Join(ids[:slots], ",") {
				t.Fatalf("claimable = %v, want the first %d ids by id", claimable, slots)
			}
			if len(decision.authorizedUnitIDs) != units || len(decision.cappedUnitIDs) != units-slots {
				t.Fatalf("authorized=%d capped=%d, want %d and %d", len(decision.authorizedUnitIDs), len(decision.cappedUnitIDs), units, units-slots)
			}
		})

		t.Run("over the total cap", func(t *testing.T) {
			decision, pages := run(t, "1000")
			if decision.allowed {
				t.Fatalf("decision = %+v, want a total-cap hard-deny", decision)
			}
			if got, want := sum(pages), 2*units; got != want {
				t.Fatalf("rows read = %d over pages %v, want %d (the cap decides after pass 1 and the suffix walk, so pass 2 never runs)", got, pages, want)
			}
			if strings.Join(decision.cappedUnitIDs, ",") != strings.Join(ids[1000:], ",") {
				t.Fatalf("cappedUnitIDs = %v, want the id-ordered suffix past 1000", decision.cappedUnitIDs)
			}
		})
	})
}

func containsID(ids []string, want string) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}
