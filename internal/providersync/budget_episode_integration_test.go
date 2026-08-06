//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestCompleteAndReleaseObserveTheBudgetEpisodeContract is the REACHED-STATE
// proof for CHAOS-3427 obligations #1 and D. The SQL-string assertions in
// repository_postgres_sql_test.go show the statements SAY the right thing;
// this shows real PostgreSQL actually DID it.
//
// Every unit is seeded mid-episode -- nonzero budget_deferrals, a
// budget_first_deferred_at, a running first_blocked_at -- because a clear that
// is asserted against an already-zero row proves nothing.
func TestCompleteAndReleaseObserveTheBudgetEpisodeContract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
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
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)

	t.Run("success clears the whole episode", func(t *testing.T) {
		seedBudgetEpisode(t, ctx, pool, firstUnitID, now)
		claim := claimForBudgetEpisode(t, ctx, repository, now)
		windowEnd := claim.BeforeAt
		if windowEnd == nil {
			t.Fatal("seeded unit has no before_at; the coverage bound cannot be exercised")
		}
		watermark := *windowEnd
		if err := repository.Complete(
			ctx, claim, map[string]any{"records": 1}, &watermark,
			now, now.Add(time.Second),
		); err != nil {
			t.Fatal(err)
		}
		state := readBudgetEpisode(t, ctx, pool, firstUnitID)
		if state.status != "success" {
			t.Fatalf("status = %q, want success", state.status)
		}
		if state.budgetDeferrals != 0 || state.budgetFirstDeferredAt != nil {
			t.Fatalf("SUCCESS left the budget episode running: deferrals=%d "+
				"first_deferred_at=%v -- the unit's next budget deferral would "+
				"inherit a resolved episode's count and wall clock",
				state.budgetDeferrals, state.budgetFirstDeferredAt)
		}
		if state.rateLimitDeferrals != 0 || state.rateLimitFirstSeenAt != nil {
			t.Fatalf("SUCCESS left the rate-limit episode running: deferrals=%d "+
				"first_seen_at=%v", state.rateLimitDeferrals, state.rateLimitFirstSeenAt)
		}
		if state.firstBlockedAt != nil {
			t.Fatalf("SUCCESS left the aggregate blocked clock running (%v); the "+
				"unit got through, so it is not going nowhere any more",
				state.firstBlockedAt)
		}
	})

	t.Run("release for retry preserves the episode", func(t *testing.T) {
		seedBudgetEpisode(t, ctx, pool, firstUnitID, now)
		claim := claimForBudgetEpisode(t, ctx, repository, now)
		if err := repository.ReleaseForRetry(ctx, claim, now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		state := readBudgetEpisode(t, ctx, pool, firstUnitID)
		if state.status != "dispatching" {
			t.Fatalf("status = %q, want dispatching", state.status)
		}
		// The deliberate asymmetry: 'provider_unit_retryable' is not an
		// episode boundary, so a still-running episode must survive intact.
		if state.budgetDeferrals != seededBudgetDeferrals || state.budgetFirstDeferredAt == nil {
			t.Fatalf("release for retry reset the budget episode: deferrals=%d "+
				"first_deferred_at=%v, want %d and a preserved stamp",
				state.budgetDeferrals, state.budgetFirstDeferredAt, seededBudgetDeferrals)
		}
		if state.firstBlockedAt == nil {
			t.Fatalf("release for retry cleared the aggregate blocked clock")
		}
		// The forbidden pattern, observed on the stored document rather than
		// on the SQL text: the prior category must be GONE, replaced by this
		// stamp's own cause.
		if state.errorCategory != "provider_unit_retryable" {
			t.Fatalf("result.error_category = %q, want provider_unit_retryable: a "+
				"preserved stale category makes stale budget counters "+
				"terminalization-eligible", state.errorCategory)
		}
	})

	t.Run("watermark write is bounded by the window end", func(t *testing.T) {
		seedBudgetEpisode(t, ctx, pool, firstUnitID, now)
		claim := claimForBudgetEpisode(t, ctx, repository, now)
		windowEnd := *claim.BeforeAt
		// A provider-derived watermark PAST the window end but still in the
		// past: the `now` ceiling alone does not catch this, which is exactly
		// why C10(c) needs the coverage bound as a second ceiling.
		overclaim := windowEnd.Add(5 * time.Hour)
		completedAt := now.Add(time.Second)
		if !overclaim.Before(completedAt) {
			t.Fatalf("the overclaiming watermark %s is not in the past relative to "+
				"the write instant %s; the `now` ceiling would catch it and this "+
				"case would stop measuring the coverage ceiling", overclaim, completedAt)
		}
		if err := repository.Complete(
			ctx, claim, map[string]any{"records": 1}, &overclaim,
			windowEnd, completedAt,
		); err != nil {
			t.Fatal(err)
		}
		stored := readWatermark(t, ctx, pool, "commits")
		if !stored.Equal(windowEnd) {
			t.Fatalf("stored watermark = %s, want the window end %s: a stamp "+
				"beyond what the unit fetched silently skips every record in "+
				"between on the next run", stored, windowEnd)
		}
	})

	t.Run("a corrupt future watermark heals downward", func(t *testing.T) {
		seedBudgetEpisode(t, ctx, pool, firstUnitID, now)
		claim := claimForBudgetEpisode(t, ctx, repository, now)
		windowEnd := *claim.BeforeAt
		// Seed the corrupt state by writing the row DIRECTLY. Going through
		// Complete would be clamped by the boundary that now works, and the
		// test would silently stop exercising the corrupt path -- the Python
		// lane caught one of its own tests going vacuous exactly this way.
		future := windowEnd.Add(365 * 24 * time.Hour)
		if _, err := pool.Exec(ctx, `
UPDATE public.sync_watermarks SET last_synced_at = $1
WHERE org_id = 'org-acme' AND source_id = 'acme/api' AND dataset_key = 'commits'`,
			future); err != nil {
			t.Fatal(err)
		}
		// Precondition: without this, a seed that silently failed to store a
		// meaningfully-future value would make the assertion below vacuous.
		if seeded := readWatermark(t, ctx, pool, "commits"); !seeded.Equal(future) {
			t.Fatalf("seed did not store the corrupt future value: got %s, want %s",
				seeded, future)
		}
		sane := windowEnd
		if err := repository.Complete(
			ctx, claim, map[string]any{"records": 1}, &sane,
			windowEnd, windowEnd.Add(time.Second),
		); err != nil {
			t.Fatal(err)
		}
		stored := readWatermark(t, ctx, pool, "commits")
		if !stored.Equal(windowEnd) {
			t.Fatalf("stored watermark = %s, want the healed value %s: without the "+
				"narrow future exception the monotonic write discards the repair "+
				"and every tick re-syncs a recovery window forever",
				stored, windowEnd)
		}
	})

	t.Run("a legitimate lower value is still refused", func(t *testing.T) {
		seedBudgetEpisode(t, ctx, pool, firstUnitID, now)
		claim := claimForBudgetEpisode(t, ctx, repository, now)
		windowEnd := *claim.BeforeAt
		if _, err := pool.Exec(ctx, `
UPDATE public.sync_watermarks SET last_synced_at = $1
WHERE org_id = 'org-acme' AND source_id = 'acme/api' AND dataset_key = 'commits'`,
			windowEnd); err != nil {
			t.Fatal(err)
		}
		// An out-of-order result from an earlier window. The stored value is
		// legitimate (not in the future), so CHAOS-2578 monotonicity applies
		// and this must NOT roll it back. Widening the future exception to
		// "any lower value wins" breaks exactly here.
		late := windowEnd.Add(-48 * time.Hour)
		if err := repository.Complete(
			ctx, claim, map[string]any{"records": 1}, &late,
			windowEnd, windowEnd.Add(time.Second),
		); err != nil {
			t.Fatal(err)
		}
		stored := readWatermark(t, ctx, pool, "commits")
		if !stored.Equal(windowEnd) {
			t.Fatalf("stored watermark = %s, want %s: a late, out-of-order result "+
				"must never roll a legitimate watermark backwards", stored, windowEnd)
		}
	})
}

const seededBudgetDeferrals = 4

type budgetEpisodeState struct {
	status                string
	rateLimitDeferrals    int
	rateLimitFirstSeenAt  *time.Time
	budgetDeferrals       int
	budgetFirstDeferredAt *time.Time
	firstBlockedAt        *time.Time
	errorCategory         string
}

// seedBudgetEpisode puts the fixture unit back into a mid-episode dispatching
// state with a PRIOR error_category the stamps under test must overwrite.
func seedBudgetEpisode(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unitID string, now time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_units
SET status = 'dispatching', lease_owner = NULL, lease_expires_at = NULL,
    available_at = NULL, duration_seconds = NULL, error = NULL,
    -- The window END sits six hours BEHIND the write instant on purpose: the
    -- coverage ceiling only proves anything when the overclaiming value is
    -- itself in the past, i.e. when the now ceiling alone would let it through.
    since_at = $2, before_at = $4,
    result = '{"error_category":"budget_deferred","go_effect_ledger_v1":{"kept":true}}'::json,
    rate_limit_deferrals = 3, rate_limit_first_seen_at = $4,
    budget_deferrals = $3, budget_first_deferred_at = $4, first_blocked_at = $4,
    updated_at = $4
WHERE id = $1`, unitID, now.Add(-30*24*time.Hour), seededBudgetDeferrals,
		now.Add(-6*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_runs SET status = 'running' WHERE id = $1`, firstRunID); err != nil {
		t.Fatal(err)
	}
}

func claimForBudgetEpisode(t *testing.T, ctx context.Context, repository *PostgresRepository, now time.Time) Claim {
	t.Helper()
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, Owner: uuid.NewString(), OrgID: "org-acme",
		Now: now, LeaseDuration: 10 * time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return claim
}

func readBudgetEpisode(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unitID string) budgetEpisodeState {
	t.Helper()
	var state budgetEpisodeState
	var category *string
	if err := pool.QueryRow(ctx, `
SELECT status, rate_limit_deferrals, rate_limit_first_seen_at,
       budget_deferrals, budget_first_deferred_at, first_blocked_at,
       result::jsonb ->> 'error_category'
FROM public.sync_run_units WHERE id = $1`, unitID).Scan(
		&state.status, &state.rateLimitDeferrals, &state.rateLimitFirstSeenAt,
		&state.budgetDeferrals, &state.budgetFirstDeferredAt, &state.firstBlockedAt,
		&category,
	); err != nil {
		t.Fatal(err)
	}
	if category != nil {
		state.errorCategory = *category
	}
	return state
}

func readWatermark(t *testing.T, ctx context.Context, pool *pgxpool.Pool, dataset string) time.Time {
	t.Helper()
	var stored time.Time
	if err := pool.QueryRow(ctx, `
SELECT last_synced_at FROM public.sync_watermarks
WHERE org_id = 'org-acme' AND source_id = 'acme/api' AND dataset_key = $1`,
		dataset).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	return stored.UTC()
}
