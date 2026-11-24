//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

type storedRefundUnit struct {
	attempts int
	status   string
}

func readRefundUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool) storedRefundUnit {
	t.Helper()
	var stored storedRefundUnit
	if err := pool.QueryRow(ctx, `SELECT attempts, status FROM public.sync_run_units WHERE id = $1`,
		firstUnitID).Scan(&stored.attempts, &stored.status); err != nil {
		t.Fatal(err)
	}
	return stored
}

func setRefundRunAge(t *testing.T, ctx context.Context, pool *pgxpool.Pool, now time.Time, age time.Duration) {
	t.Helper()
	if _, err := pool.Exec(ctx, `UPDATE public.sync_runs SET created_at = $2 WHERE id = $1`,
		firstRunID, now.Add(-age)); err != nil {
		t.Fatal(err)
	}
}

// TestChunkProgressRefundAttemptsWriters (CHAOS-14574) runs every writer of
// sync_run_units.attempts the provider unit's retry ladder uses against
// Postgres, one subtest per writer: claim +1, plain chunk continuation -1 (no
// progress or age check), progress refund -1 only with durable checkpoint
// progress past the attempt's start in the unit's own generation and a run
// younger than 24 h, release for retry unchanged.
func TestChunkProgressRefundAttemptsWriters(t *testing.T) {
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
	// Each subtest starts from a freshly seeded dispatching unit, so a red in
	// one writer's subtest cannot cascade into another's.
	fresh := func(t *testing.T, runAge time.Duration) (Claim, int) {
		t.Helper()
		seedBudgetEpisode(t, ctx, pool, firstUnitID, now)
		setRefundRunAge(t, ctx, pool, now, runAge)
		base := readRefundUnit(t, ctx, pool).attempts
		return claimForBudgetEpisode(t, ctx, repository, now), base
	}

	t.Run("claim_adds_one", func(t *testing.T) {
		_, base := fresh(t, time.Hour)
		if got := readRefundUnit(t, ctx, pool); got.attempts != base+1 || got.status != "running" {
			t.Fatalf("after claim: %+v, want attempts %d running", got, base+1)
		}
	})

	t.Run("plain_continuation_subtracts_one_without_progress_or_age_check", func(t *testing.T) {
		claim, base := fresh(t, 25*time.Hour)
		if _, err := pool.Exec(ctx, `DELETE FROM public.sync_run_unit_chunk_checkpoints WHERE sync_run_unit_id = $1::uuid`,
			firstUnitID); err != nil {
			t.Fatal(err)
		}
		if err := repository.DeferChunkContinuation(ctx, claim, now.Add(time.Second), now); err != nil {
			t.Fatalf("plain continuation with no checkpoint and a 25 h old run: %v", err)
		}
		if got := readRefundUnit(t, ctx, pool); got.attempts != base || got.status != "dispatching" {
			t.Fatalf("after continuation: %+v, want attempts %d dispatching", got, base)
		}
	})

	t.Run("refund_without_progress_writes_nothing", func(t *testing.T) {
		claim, base := fresh(t, time.Hour)
		putRateLimitTestCheckpoint(t, ctx, pool, claim.GenerationKey(), 5, 5, now)
		refunded, err := repository.RefundChunkProgress(ctx, claim, 5, now.Add(5*time.Second), now)
		if err != nil || refunded {
			t.Fatalf("checkpoint at the start ordinal: refunded=%v err=%v, want no refund", refunded, err)
		}
		putRateLimitTestCheckpoint(t, ctx, pool, "sync-unit:other-generation", 9, 9, now)
		refunded, err = repository.RefundChunkProgress(ctx, claim, 5, now.Add(5*time.Second), now)
		if err != nil || refunded {
			t.Fatalf("progress only in another generation: refunded=%v err=%v, want no refund", refunded, err)
		}
		if got := readRefundUnit(t, ctx, pool); got.attempts != base+1 || got.status != "running" {
			t.Fatalf("after refused refunds: %+v, want attempts %d running", got, base+1)
		}
	})

	t.Run("refund_past_the_run_age_cap_writes_nothing", func(t *testing.T) {
		claim, base := fresh(t, 24*time.Hour)
		putRateLimitTestCheckpoint(t, ctx, pool, claim.GenerationKey(), 6, 6, now)
		refunded, err := repository.RefundChunkProgress(ctx, claim, 5, now.Add(5*time.Second), now)
		if err != nil || refunded {
			t.Fatalf("checkpoint 5 -> 6, run 24 h old: refunded=%v err=%v, want no refund", refunded, err)
		}
		if got := readRefundUnit(t, ctx, pool); got.attempts != base+1 || got.status != "running" {
			t.Fatalf("after capped refund: %+v, want attempts %d running", got, base+1)
		}
	})

	t.Run("refund_with_progress_subtracts_one", func(t *testing.T) {
		claim, base := fresh(t, 23*time.Hour)
		putRateLimitTestCheckpoint(t, ctx, pool, claim.GenerationKey(), 6, 6, now)
		refunded, err := repository.RefundChunkProgress(ctx, claim, 5, now.Add(5*time.Second), now)
		if err != nil || !refunded {
			t.Fatalf("checkpoint 5 -> 6, run 23 h old: refunded=%v err=%v, want a refund", refunded, err)
		}
		if got := readRefundUnit(t, ctx, pool); got.attempts != base || got.status != "dispatching" {
			t.Fatalf("after refund: %+v, want attempts %d dispatching", got, base)
		}
	})

	t.Run("release_for_retry_keeps_attempts", func(t *testing.T) {
		claim, base := fresh(t, time.Hour)
		if err := repository.ReleaseForRetry(ctx, claim, now); err != nil {
			t.Fatal(err)
		}
		if got := readRefundUnit(t, ctx, pool); got.attempts != base+1 || got.status != "dispatching" {
			t.Fatalf("after claim + release: %+v, want attempts %d dispatching", got, base+1)
		}
	})
}
