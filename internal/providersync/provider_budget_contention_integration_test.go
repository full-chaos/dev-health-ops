//go:build integration

package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestProviderBudgetContentionSurvivesRestartWithoutArmingFinalization proves
// the reached PostgreSQL state, not only the SQL text. The first repository
// writes the deferral; a newly constructed repository cannot claim it before
// available_at, can claim it after, and no finalize row appears while the unit
// remains nonterminal.
func TestProviderBudgetContentionSurvivesRestartWithoutArmingFinalization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)

	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	priorEpisodeAt := now.Add(-10 * time.Minute)
	const priorAttempts = 3
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_units
SET attempts = $2,
    result = '{"error_category":"budget_deferred","sentinel":{"kept":true},"provider_budget_contention_deferrals":2}'::json,
    rate_limit_deferrals = 4,
    rate_limit_first_seen_at = $3,
    budget_deferrals = 5,
    budget_first_deferred_at = $3,
    first_blocked_at = $3
WHERE id = $1`, firstUnitID, priorAttempts, priorEpisodeAt); err != nil {
		t.Fatal(err)
	}
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	availableAt := now.Add(1500 * time.Millisecond)
	if err := repository.DeferForBudgetContention(ctx, claim, availableAt, now); err != nil {
		t.Fatal(err)
	}

	var status, category, retryReason string
	var storedAvailable time.Time
	var attempts, contentionDeferrals, rateLimitDeferrals, intrinsicDeferrals, finalizeRows int
	var rateLimitFirstSeen, intrinsicFirstDeferred, firstBlocked *time.Time
	var sentinelKept bool
	if err := pool.QueryRow(ctx, `
SELECT status, available_at, result::jsonb ->> 'error_category',
       (result::jsonb ->> 'provider_budget_contention_deferrals')::integer,
       COALESCE((result::jsonb #>> '{sentinel,kept}')::boolean, false),
       attempts, rate_limit_deferrals, rate_limit_first_seen_at,
       budget_deferrals, budget_first_deferred_at, first_blocked_at,
       COALESCE(last_retry_reason, '')
FROM public.sync_run_units WHERE id = $1`, firstUnitID).Scan(
		&status, &storedAvailable, &category, &contentionDeferrals,
		&sentinelKept, &attempts, &rateLimitDeferrals, &rateLimitFirstSeen,
		&intrinsicDeferrals, &intrinsicFirstDeferred, &firstBlocked, &retryReason,
	); err != nil {
		t.Fatal(err)
	}
	if status != "dispatching" || !storedAvailable.Equal(availableAt) ||
		category != "provider_budget_contention" || contentionDeferrals != 3 ||
		retryReason != "provider_budget_contention" {
		t.Fatalf("durable contention state=%q %v %q %d %q",
			status, storedAvailable, category, contentionDeferrals, retryReason)
	}
	if !sentinelKept || attempts != priorAttempts || rateLimitDeferrals != 4 ||
		rateLimitFirstSeen == nil || !rateLimitFirstSeen.Equal(priorEpisodeAt) ||
		intrinsicDeferrals != 5 || intrinsicFirstDeferred == nil ||
		!intrinsicFirstDeferred.Equal(priorEpisodeAt) || firstBlocked == nil ||
		!firstBlocked.Equal(priorEpisodeAt) {
		t.Fatalf("preserved state=sentinel:%t attempts:%d rate:%d/%v budget:%d/%v blocked:%v",
			sentinelKept, attempts, rateLimitDeferrals, rateLimitFirstSeen,
			intrinsicDeferrals, intrinsicFirstDeferred, firstBlocked)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM public.sync_dispatch_outbox
WHERE sync_run_id = $1::uuid AND kind = 'finalize_sync_run'`, claim.SyncRunID).Scan(&finalizeRows); err != nil {
		t.Fatal(err)
	}
	if finalizeRows != 0 {
		t.Fatalf("contention armed %d finalize rows while work remains", finalizeRows)
	}

	restarted, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	_, err = restarted.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(),
		Now: availableAt.Add(-time.Millisecond), LeaseDuration: time.Minute,
		AllowExpiredRecovery: true,
	})
	if !errors.Is(err, ErrUnitNotClaimable) {
		t.Fatalf("restart claimed before durable available_at: %v", err)
	}
	reclaimed, err := restarted.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(),
		Now: availableAt, LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatalf("restart could not claim at durable available_at: %v", err)
	}
	if reclaimed.Attempt != priorAttempts+1 {
		t.Fatalf("post-deferral attempts=%d, want %d", reclaimed.Attempt, priorAttempts+1)
	}
}
