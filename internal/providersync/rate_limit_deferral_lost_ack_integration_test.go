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

// TestDeferralWritesAreReadBackAfterALostCommitAcknowledgement runs the
// rate-limit and the budget-contention deferrals against Postgres with the COMMIT
// acknowledgement lost: the write is read back, the attempt was given back once,
// the observation row exists exactly once, and no retry ran.
func TestDeferralWritesAreReadBackAfterALostCommitAcknowledgement(t *testing.T) {
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
		if _, err := pool.Exec(ctx, `DELETE FROM public.provider_rate_limit_observations WHERE sync_run_unit_id = $1::uuid`, firstUnitID); err != nil {
			t.Fatal(err)
		}
		base := readRefundUnit(t, ctx, pool).attempts
		return claimForBudgetEpisode(t, ctx, repository, now), base
	}
	observations := func(t *testing.T) int {
		t.Helper()
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.provider_rate_limit_observations WHERE sync_run_unit_id = $1::uuid`,
			firstUnitID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}

	t.Run("rate limit: committed, reply lost", func(t *testing.T) {
		claim, base := fresh(t)
		sleeps := countRetrySleeps(repository)
		commits := 0
		repository.commitTx = lostAckAfterCommit(&commits)
		defer func() { repository.commitTx = nil }()
		err := repository.DeferForRateLimit(ctx, claim, now.Add(time.Minute), now,
			RateLimitObservation{Dimension: "rest_core", RetryAfter: time.Minute}, RateLimitEpisodeStep{})
		if err != nil {
			t.Fatalf("DeferForRateLimit = %v, want the read-back to find the commit", err)
		}
		if commits != 1 || *sleeps != 0 {
			t.Fatalf("commits=%d waits=%d, want one commit and no retry", commits, *sleeps)
		}
		if got := readRefundUnit(t, ctx, pool); got.attempts != base || got.status != "dispatching" {
			t.Fatalf("after the deferral: %+v, want attempts %d dispatching", got, base)
		}
		if count := observations(t); count != 1 {
			t.Fatalf("%d observation rows, want exactly 1", count)
		}
	})

	t.Run("rate limit: rolled back, nothing committed", func(t *testing.T) {
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
		err := repository.DeferForRateLimit(ctx, claim, now.Add(time.Minute), now,
			RateLimitObservation{Dimension: "rest_core", RetryAfter: time.Minute}, RateLimitEpisodeStep{})
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("DeferForRateLimit = %v, want ErrLeaseLost when nothing committed", err)
		}
		if commits != storeWriteAttempts || *sleeps != storeWriteAttempts-1 {
			t.Fatalf("commits=%d waits=%d, want %d attempts with %d waits", commits, *sleeps, storeWriteAttempts, storeWriteAttempts-1)
		}
		if got := readRefundUnit(t, ctx, pool); got.attempts != base+1 || got.status != "running" {
			t.Fatalf("after the rollbacks: %+v, want attempts %d running", got, base+1)
		}
		if count := observations(t); count != 0 {
			t.Fatalf("%d observation rows, want none", count)
		}
	})

	t.Run("budget contention: committed, reply lost", func(t *testing.T) {
		claim, base := fresh(t)
		sleeps := countRetrySleeps(repository)
		commits := 0
		repository.commitTx = lostAckAfterCommit(&commits)
		defer func() { repository.commitTx = nil }()
		deferrals, err := repository.DeferForBudgetContention(ctx, claim, now.Add(time.Second), now)
		if err != nil || deferrals != 1 {
			t.Fatalf("DeferForBudgetContention = %d, %v, want 1, nil (the read-back finds the commit)", deferrals, err)
		}
		if commits != 1 || *sleeps != 0 {
			t.Fatalf("commits=%d waits=%d, want one commit and no retry", commits, *sleeps)
		}
		if got := readRefundUnit(t, ctx, pool); got.attempts != base || got.status != "dispatching" {
			t.Fatalf("after the deferral: %+v, want attempts %d dispatching", got, base)
		}
		var stored int
		if err := pool.QueryRow(ctx, `SELECT (result::jsonb ->> 'provider_budget_contention_deferrals')::integer FROM public.sync_run_units WHERE id = $1::uuid`,
			firstUnitID).Scan(&stored); err != nil || stored != 1 {
			t.Fatalf("stored contention deferrals = %d, %v, want 1", stored, err)
		}
	})
}
