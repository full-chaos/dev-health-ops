//go:build integration

package providersync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

type lostAckRow struct {
	status    string
	attempts  int
	leaseNull bool
}

func readLostAckRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool) lostAckRow {
	t.Helper()
	var row lostAckRow
	if err := pool.QueryRow(ctx, `SELECT status, attempts, lease_owner IS NULL FROM public.sync_run_units WHERE id = $1`,
		firstUnitID).Scan(&row.status, &row.attempts, &row.leaseNull); err != nil {
		t.Fatal(err)
	}
	return row
}

// riverOutcomeOfAnError is what River does with the handler's Retryable return
// of a failed write at this attempt (registry max_attempts 5).
func riverOutcomeOfAnError(attempt int) string {
	if attempt >= 5 {
		return "discard (River job gone, budget spent)"
	}
	return "retry (River attempt charged)"
}

// THROWAWAY PROBE (CHAOS-14574 r1 opinion; never merged): the PLAIN chunk
// continuation UPDATE commits on Postgres and its reply is lost. FAILS while
// the gap exists: the write returns an error although the row committed, and
// the handler answers the error with Retryable (providerunit.go
// DeferChunkContinuation arm -> retryCauseChunkDeferFailed).
func TestProbeLostAckAfterCommittedPlainContinuation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	for _, attempt := range []int{2, 5} {
		t.Run(fmt.Sprintf("attempt_%d", attempt), func(t *testing.T) {
			now := time.Date(2026, 11, 24, 9, attempt, 0, 0, time.UTC)
			seedBudgetEpisode(t, ctx, pool, firstUnitID, now)
			if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET attempts = $2 WHERE id = $1`,
				firstUnitID, attempt-1); err != nil {
				t.Fatal(err)
			}
			control := &lostAckControl{marker: []byte("provider_unit_chunk_continuation")}
			cutPool := lostAckPool(t, ctx, instance.URI, control)
			defer cutPool.Close()
			repository, err := NewPostgresRepository(cutPool)
			if err != nil {
				t.Fatal(err)
			}
			claim := claimForBudgetEpisode(t, ctx, repository, now)
			control.mu.Lock()
			control.armed = true
			control.mu.Unlock()
			writeErr := repository.DeferChunkContinuation(ctx, claim, now.Add(time.Second), now)
			row := readLostAckRow(t, ctx, pool)
			t.Logf("PLAIN continuation, River attempt %d of 5: reply cut=%d write_err=%v -> handler Retryable -> %s; "+
				"stored unit status=%q attempts=%d (claimed at %d) lease_owner_null=%v",
				attempt, control.cuts, writeErr, riverOutcomeOfAnError(attempt), row.status, row.attempts, attempt, row.leaseNull)
			if control.cuts != 1 {
				t.Fatalf("probe premise: the reply was not cut (cuts=%d)", control.cuts)
			}
			if writeErr != nil && row.status == "dispatching" && row.attempts == attempt-1 && row.leaseNull {
				t.Errorf("GAP: the continuation COMMITTED (status=%q attempts=%d lease cleared) but returned %v; "+
					"River outcome %s", row.status, row.attempts, writeErr, riverOutcomeOfAnError(attempt))
			}
		})
	}
}
