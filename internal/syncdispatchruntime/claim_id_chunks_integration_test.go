//go:build integration

package syncdispatchruntime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// claimOutcome claims in a transaction that is rolled back, so the same rows can be claimed again with another chunk size.
func claimOutcome(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authorizedIDs, cappedIDs []string, now time.Time) (string, string) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	units, deferred, err := claimUnits(ctx, tx, budgetCandidatesRunID, idMap(authorizedIDs), idMap(cappedIDs), now)
	if err != nil {
		t.Fatalf("claimUnits: %v", err)
	}
	claimed := make([]string, 0, len(units))
	for _, unit := range units {
		claimed = append(claimed, unit.id)
	}
	sort.Strings(claimed)
	sort.Strings(deferred)
	return strings.Join(claimed, ","), strings.Join(deferred, ",")
}

// Chunking the id binds claims the same units and reports the same units outside the snapshot, with the authorized and
// the capped ids each one below, at and one above the chunk size.
func TestClaimUnitsChunkedBindsClaimTheSameUnitsAsOneBind(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		const size = 5
		now := pgNow()
		past := now.Add(-time.Hour)
		due := now.Add(-time.Minute)
		const total = 3*size + 1
		ids := make([]string, total)
		for index := range ids {
			ids[index] = fmt.Sprintf("00000000-0000-4000-8000-00000000%04x", 0x700+index)
			fixture := candidateUnitFixture{id: ids[index], updatedAt: now}
			switch index % 3 {
			case 0:
				fixture.status = syncRunUnitStatusPlanned
			case 1:
				fixture.status, fixture.availableAt = syncRunUnitStatusRetrying, &due
			default:
				fixture.status, fixture.updatedAt = syncRunUnitStatusDispatching, past
			}
			insertCandidateUnit(t, ctx, pool, fixture)
		}
		for _, authorizedCount := range []int{size - 1, size, size + 1, total} {
			for _, cappedCount := range []int{0, size - 1, size, size + 1} {
				if cappedCount > authorizedCount {
					continue
				}
				t.Run(fmt.Sprintf("authorized=%d capped=%d", authorizedCount, cappedCount), func(t *testing.T) {
					authorizedIDs, cappedIDs := ids[:authorizedCount], ids[:cappedCount]
					wantClaimed, wantDeferred := claimOutcome(t, ctx, pool, authorizedIDs, cappedIDs, now)
					withClaimChunkSize(t, size)
					gotClaimed, gotDeferred := claimOutcome(t, ctx, pool, authorizedIDs, cappedIDs, now)
					if gotClaimed != wantClaimed || gotDeferred != wantDeferred {
						t.Fatalf("chunked claim differs\nclaimed  got:  %s\n         want: %s\ndeferred got:  %s\n         want: %s",
							gotClaimed, wantClaimed, gotDeferred, wantDeferred)
					}
					if authorizedCount > cappedCount && wantClaimed == "" {
						t.Fatal("the reference claim took nothing, so this case proves nothing")
					}
				})
			}
		}
	})
}
