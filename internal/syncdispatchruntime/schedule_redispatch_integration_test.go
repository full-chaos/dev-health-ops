//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func dispatchOutboxAvailableAt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, syncRunID string) time.Time {
	t.Helper()
	var availableAt time.Time
	if err := pool.QueryRow(ctx, `SELECT available_at FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind=$2`,
		syncRunID, outboxKindDispatchSyncRun).Scan(&availableAt); err != nil {
		t.Fatal(err)
	}
	return availableAt
}

// TestScheduleRedispatchArmsANewWakeupWithTheDefaultCountdown pins the
// no-explicit-available_at path: a fresh wakeup is armed at
// now+SYNC_DISPATCH_REDISPATCH_COUNTDOWN (default 60s).
func TestScheduleRedispatchArmsANewWakeupWithTheDefaultCountdown(t *testing.T) {
	withDispatchGatePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()
		scheduleRedispatch(ctx, pool, nil, discoveryTestRun, nil, now)

		got := dispatchOutboxAvailableAt(t, ctx, pool, discoveryTestRun)
		want := now.Add(60 * time.Second)
		if got.Sub(want).Abs() > time.Second {
			t.Fatalf("available_at=%s want~=%s (default 60s countdown)", got, want)
		}
	})
}

// TestScheduleRedispatchUsesTheExplicitAvailableAtWhenGiven pins the
// explicit-deferral path: availableAt, when provided, is used verbatim
// instead of the countdown.
func TestScheduleRedispatchUsesTheExplicitAvailableAtWhenGiven(t *testing.T) {
	withDispatchGatePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()
		explicit := now.Add(45 * time.Minute)
		scheduleRedispatch(ctx, pool, nil, discoveryTestRun, &explicit, now)

		got := dispatchOutboxAvailableAt(t, ctx, pool, discoveryTestRun)
		if !got.Equal(explicit) {
			t.Fatalf("available_at=%s want=%s", got, explicit)
		}
	})
}

// TestScheduleRedispatchOverwritesAPendingUnclaimedRowsAvailableAt pins
// the second, unconditional-overwrite write: an EXISTING pending,
// unclaimed row's available_at moves to the newly-computed value even
// when that value is LATER than what is already there -- distinct from
// the upsert's own LEAST-semantics merge, which would otherwise keep the
// earlier of the two.
func TestScheduleRedispatchOverwritesAPendingUnclaimedRowsAvailableAt(t *testing.T) {
	withDispatchGatePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()
		earlier := now.Add(5 * time.Minute)
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_dispatch_outbox (id,sync_run_id,org_id,kind,status,available_at,created_at,updated_at)
VALUES ($1,$2,$3,$4,'pending',$5,now(),now())`,
			"00000000-0000-4000-8000-0000000000d5", discoveryTestRun, discoveryTestOrg, outboxKindDispatchSyncRun, earlier); err != nil {
			t.Fatal(err)
		}

		later := now.Add(45 * time.Minute)
		scheduleRedispatch(ctx, pool, nil, discoveryTestRun, &later, now)

		got := dispatchOutboxAvailableAt(t, ctx, pool, discoveryTestRun)
		if !got.Equal(later) {
			t.Fatalf("available_at=%s want=%s -- the explicit re-arm must overwrite even a LATER-than-earlier value, not merge via LEAST", got, later)
		}
	})
}

// TestScheduleRedispatchDoesNotOverwriteAClaimedRow pins the second
// write's own predicate: a row already claimed (claim_token set, with a
// live, not-yet-expired claim_expires_at -- a claim token with no expiry
// is not a claim the upsert's own CASE logic recognizes as live, and it
// clears the token outright) is left untouched -- a claim in flight must
// not have its target time yanked out from under it.
func TestScheduleRedispatchDoesNotOverwriteAClaimedRow(t *testing.T) {
	withDispatchGatePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()
		claimedAt := now.Add(5 * time.Minute)
		claimExpiresAt := now.Add(time.Hour)
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_dispatch_outbox (id,sync_run_id,org_id,kind,status,available_at,claim_token,claim_expires_at,created_at,updated_at)
VALUES ($1,$2,$3,$4,'pending',$5,'some-claim-token',$6,now(),now())`,
			"00000000-0000-4000-8000-0000000000d6", discoveryTestRun, discoveryTestOrg, outboxKindDispatchSyncRun, claimedAt, claimExpiresAt); err != nil {
			t.Fatal(err)
		}

		later := now.Add(45 * time.Minute)
		scheduleRedispatch(ctx, pool, nil, discoveryTestRun, &later, now)

		got := dispatchOutboxAvailableAt(t, ctx, pool, discoveryTestRun)
		if !got.Equal(claimedAt) {
			t.Fatalf("available_at=%s want=%s (unchanged) -- a claimed row must not be re-armed out from under the claimant", got, claimedAt)
		}
	})
}

// TestScheduleRedispatchNeverPanicsOrPropagatesWhenTheTransactionFails
// pins the exception-swallowing property empirically: with the pool
// already closed (forcing pool.Begin to fail), the call must return
// cleanly -- no panic, and (being a bare function call with no error
// return) structurally nothing to propagate.
func TestScheduleRedispatchNeverPanicsOrPropagatesWhenTheTransactionFails(t *testing.T) {
	withDispatchGatePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		pool.Close()
		now := time.Now().UTC()
		scheduleRedispatch(ctx, pool, nil, discoveryTestRun, nil, now) // must not panic
	})
}
