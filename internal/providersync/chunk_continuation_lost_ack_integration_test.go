//go:build integration

package providersync

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestChunkContinuationWritesAreReadBackAfterALostCommitAcknowledgement runs the
// continuation and the progress refund against Postgres with the COMMIT
// acknowledgement lost (the transaction commits, the caller sees a lost
// connection): the write is read back, the unit's attempts went down once, and
// the commit step ran once with no retry.
func TestChunkContinuationWritesAreReadBackAfterALostCommitAcknowledgement(t *testing.T) {
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
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	fresh := func(t *testing.T) (Claim, int) {
		t.Helper()
		seedBudgetEpisode(t, ctx, pool, firstUnitID, now)
		setRefundRunAge(t, ctx, pool, now, time.Hour)
		base := readRefundUnit(t, ctx, pool).attempts
		return claimForBudgetEpisode(t, ctx, repository, now), base
	}

	t.Run("continuation: committed, reply lost", func(t *testing.T) {
		claim, base := fresh(t)
		sleeps := countRetrySleeps(repository)
		commits := 0
		repository.commitTx = lostAckAfterCommit(&commits)
		defer func() { repository.commitTx = nil }()
		if err := repository.DeferChunkContinuation(ctx, claim, now.Add(time.Second), now); err != nil {
			t.Fatalf("DeferChunkContinuation = %v, want the read-back to find the commit", err)
		}
		if commits != 1 || *sleeps != 0 {
			t.Fatalf("commits=%d waits=%d, want one commit and no retry", commits, *sleeps)
		}
		if got := readRefundUnit(t, ctx, pool); got.attempts != base || got.status != "dispatching" {
			t.Fatalf("after the continuation: %+v, want attempts %d dispatching", got, base)
		}
	})

	t.Run("refund: committed, reply lost", func(t *testing.T) {
		claim, base := fresh(t)
		putRateLimitTestCheckpoint(t, ctx, pool, claim.GenerationKey(), 6, 6, now)
		sleeps := countRetrySleeps(repository)
		commits := 0
		repository.commitTx = lostAckAfterCommit(&commits)
		defer func() { repository.commitTx = nil }()
		refunded, err := repository.RefundChunkProgress(ctx, claim, 5, now.Add(5*time.Second), now)
		if err != nil || !refunded {
			t.Fatalf("RefundChunkProgress = %v, %v, want true, nil (the read-back finds the commit)", refunded, err)
		}
		if commits != 1 || *sleeps != 0 {
			t.Fatalf("commits=%d waits=%d, want one commit and no retry", commits, *sleeps)
		}
		if got := readRefundUnit(t, ctx, pool); got.attempts != base || got.status != "dispatching" {
			t.Fatalf("after the refund: %+v, want attempts %d dispatching", got, base)
		}
	})

	t.Run("continuation: rolled back, nothing committed", func(t *testing.T) {
		claim, base := fresh(t)
		sleeps := countRetrySleeps(repository)
		commits := 0
		repository.commitTx = func(ctx context.Context, tx pgx.Tx) error {
			commits++
			if err := tx.Rollback(ctx); err != nil {
				return err
			}
			return io.ErrUnexpectedEOF
		}
		defer func() { repository.commitTx = nil }()
		err := repository.DeferChunkContinuation(ctx, claim, now.Add(time.Second), now)
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("DeferChunkContinuation = %v, want ErrLeaseLost when nothing committed", err)
		}
		if commits != storeWriteAttempts || *sleeps != storeWriteAttempts-1 {
			t.Fatalf("commits=%d waits=%d, want %d attempts with %d waits", commits, *sleeps, storeWriteAttempts, storeWriteAttempts-1)
		}
		if got := readRefundUnit(t, ctx, pool); got.attempts != base+1 || got.status != "running" {
			t.Fatalf("after the rollbacks: %+v, want attempts %d running", got, base+1)
		}
	})
}
