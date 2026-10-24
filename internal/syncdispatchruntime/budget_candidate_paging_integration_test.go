//go:build integration

package syncdispatchruntime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// More candidates than one page come back whole and in id order through the real keyset statement.
func TestDispatchCandidateUnitsReadsMoreThanAPageInIDOrder(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		n := unitPageSize + 5
		for index := 1; index <= n; index++ {
			insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: fmt.Sprintf("00000000-0000-4000-8000-%012d", index), status: syncRunUnitStatusPlanned, updatedAt: now})
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		ignored := map[string]bool{fmt.Sprintf("00000000-0000-4000-8000-%012d", 3): true}
		maxRows := 0
		load := loadDispatchCandidatePage(ctx, tx, budgetCandidatesRunID, now)
		var got []string
		err = pageDispatchCandidates(func(afterID string, limit int) ([]budgetUnit, error) {
			page, err := load(afterID, limit)
			maxRows = max(maxRows, len(page))
			return page, err
		}, ignored, func(page []budgetUnit) error {
			for _, unit := range page {
				got = append(got, unit.id)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != n-1 || maxRows != unitPageSize {
			t.Fatalf("got %d ids (want %d), largest statement %d rows (want %d)", len(got), n-1, maxRows, unitPageSize)
		}
		for index := 1; index < len(got); index++ {
			if got[index-1] >= got[index] {
				t.Fatalf("ids out of order at %d: %s then %s", index, got[index-1], got[index])
			}
		}
		whole, err := dispatchCandidateUnits(ctx, tx, budgetCandidatesRunID, ignored, now)
		if err != nil || len(whole) != n-1 {
			t.Fatalf("dispatchCandidateUnits = %d units, %v, want %d", len(whole), err, n-1)
		}
	})
}

// The first-16 surplus pick over the real pages is the first 16 of the SQL order, for more deferred units than a page.
func TestSurplusRetryCandidatesPickMatchesTheSQLOrder(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		future := now.Add(time.Hour)
		n := unitPageSize + 20
		for index := 1; index <= n; index++ {
			firstDeferred := now.Add(-time.Duration((index*7)%50) * time.Hour)
			fixture := candidateUnitFixture{
				id: fmt.Sprintf("00000000-0000-4000-8000-%012d", index), status: syncRunUnitStatusRetrying, availableAt: &future, updatedAt: now,
				resultJSON: `{"error_category":"budget_deferred"}`,
			}
			if index%6 != 0 {
				fixture.budgetFirstDefAt = &firstDeferred
			}
			if index%4 == 0 {
				blocked := now.Add(-time.Duration(index%5) * time.Minute)
				fixture.firstBlockedAt = &blocked
			}
			insertCandidateUnit(t, ctx, pool, fixture)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		headroom := map[dispatchBucket]int{{orgID: "org-1", provider: "github", costClass: "medium"}: 1}
		got, err := surplusRetryCandidates(ctx, tx, nil, budgetCandidatesRunID, nil, headroom, now)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := tx.Query(ctx, `
SELECT id::text FROM public.sync_run_units
WHERE sync_run_id = $1::uuid AND status = $2 AND available_at > $3
ORDER BY COALESCE(budget_first_deferred_at, '9999-01-01'::timestamptz), COALESCE(first_blocked_at, '9999-01-01'::timestamptz), id
LIMIT 16`, budgetCandidatesRunID, syncRunUnitStatusRetrying, now)
		if err != nil {
			t.Fatal(err)
		}
		var want []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			want = append(want, id)
		}
		rows.Close()
		if len(got) != 16 || len(want) != 16 {
			t.Fatalf("got %d, SQL %d, want 16", len(got), len(want))
		}
		for index := range want {
			if got[index].id != want[index] {
				t.Fatalf("pick[%d] = %s, SQL order has %s", index, got[index].id, want[index])
			}
		}
	})
}

// The invalid-claim pass terminalizes more invalid candidates than one page, page by page, and leaves the valid ones.
func TestTerminalizeInvalidClaimsAmongDispatchCandidatesPages(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		invalidCount := unitPageSize + 3
		total := invalidCount + 10
		for index := 1; index <= total; index++ {
			fixture := candidateUnitFixture{id: fmt.Sprintf("00000000-0000-4000-8000-%012d", index), status: syncRunUnitStatusPlanned, updatedAt: now}
			if index <= invalidCount {
				fixture.datasetKey = "work-item-labels"
			}
			insertCandidateUnit(t, ctx, pool, fixture)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		found, terminalized, err := terminalizeInvalidClaimsAmongDispatchCandidates(ctx, tx, budgetCandidatesRunID, now, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		if found != invalidCount || terminalized != invalidCount {
			t.Fatalf("found %d, terminalized %d, want %d each", found, terminalized, invalidCount)
		}
		var failed, planned int
		if err := tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status = $2), count(*) FILTER (WHERE status = $3) FROM public.sync_run_units WHERE sync_run_id = $1::uuid`,
			budgetCandidatesRunID, syncRunUnitStatusFailed, syncRunUnitStatusPlanned).Scan(&failed, &planned); err != nil {
			t.Fatal(err)
		}
		if failed != invalidCount || planned != total-invalidCount {
			t.Fatalf("failed=%d planned=%d, want %d and %d", failed, planned, invalidCount, total-invalidCount)
		}
	})
}
