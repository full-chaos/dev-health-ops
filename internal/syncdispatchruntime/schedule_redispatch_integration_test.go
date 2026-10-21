//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
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
		now := pgNow()
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
		now := pgNow()
		explicit := now.Add(45 * time.Minute)
		scheduleRedispatch(ctx, pool, nil, discoveryTestRun, &explicit, now)

		got := dispatchOutboxAvailableAt(t, ctx, pool, discoveryTestRun)
		if !got.Equal(explicit) {
			t.Fatalf("available_at=%s want=%s", got, explicit)
		}
	})
}

// TestScheduleRedispatchKeepsAnEarlierWakeupAnotherWriterArmed pins
// CHAOS-10013: a row that is ALREADY pending when the pass re-arms was armed by
// another writer while the pass ran (a unit leaving its slot, reference
// discovery, the materializer), so the re-arm merges by LEAST and keeps that
// earlier wakeup instead of pushing it out to the pass's own, later instant.
// The overwrite still applies to the pass's own delivered row: see
// TestScheduleRedispatchStillCountsDownFromItsOwnDeliveredRow.
//
// RED CONTROL: before CHAOS-10013 the second write overwrote this row
// unconditionally and available_at moved to the later value.
func TestScheduleRedispatchKeepsAnEarlierWakeupAnotherWriterArmed(t *testing.T) {
	withDispatchGatePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		earlier := now.Add(5 * time.Minute)
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_dispatch_outbox (id,sync_run_id,org_id,kind,status,available_at,attempts,created_at,updated_at)
VALUES ($1,$2,$3,$4,'pending',$5,0,now(),now())`,
			"00000000-0000-4000-8000-0000000000d5", discoveryTestRun, discoveryTestOrg, outboxKindDispatchSyncRun, earlier); err != nil {
			t.Fatal(err)
		}

		later := now.Add(45 * time.Minute)
		scheduleRedispatch(ctx, pool, nil, discoveryTestRun, &later, now)

		got := dispatchOutboxAvailableAt(t, ctx, pool, discoveryTestRun)
		if !got.Equal(earlier) {
			t.Fatalf("available_at=%s want=%s -- a wakeup another writer armed must survive the pass's later re-arm (LEAST), not be overwritten", got, earlier)
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
		now := pgNow()
		claimedAt := now.Add(5 * time.Minute)
		claimExpiresAt := now.Add(time.Hour)
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_dispatch_outbox (id,sync_run_id,org_id,kind,status,available_at,attempts,claim_token,claim_expires_at,created_at,updated_at)
VALUES ($1,$2,$3,$4,'pending',$5,0,'some-claim-token',$6,now(),now())`,
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
		now := pgNow()
		scheduleRedispatch(ctx, pool, nil, discoveryTestRun, nil, now) // must not panic
	})
}

func seedBackoffUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, dataset, status string, updatedAgo time.Duration) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github',$4,$5,$6,now() - make_interval(secs => $7),(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'medium','incremental',0,now() - interval '3 hours')`,
		id, discoveryTestOrg, discoveryTestRun, dataset, dispatchTestSource, status, updatedAgo.Seconds()); err != nil {
		t.Fatal(err)
	}
}

// TestRearmBackoffGrowsWhileNoUnitEndsAndResetsWhenOneEnds reads the real
// sync_run_units rows: a running unit with a fresh heartbeat is not progress,
// the last success two hours ago is, and a unit that ends now resets the
// back-off on the next arm.
func TestRearmBackoffGrowsWhileNoUnitEndsAndResetsWhenOneEnds(t *testing.T) {
	t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "10")
	t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN_MAX", "300")
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		seedBackoffUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000012a1", "files", "running", 0)
		seedBackoffUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000012a2", "commits", "success", 2*time.Hour)
		seedBackoffUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000012a3", "blame", "planned", 0)

		held, attrs := rearmBackoff(ctx, pool, nil, discoveryTestRun, time.Now().UTC())
		if held <= 10*time.Second || held > 300*time.Second || len(attrs) == 0 {
			t.Fatalf("held run got countdown=%s attrs=%d want a back-off in (10s,300s]", held, len(attrs))
		}

		if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET status='success', updated_at=now() WHERE id=$1`, "00000000-0000-4000-8000-0000000012a1"); err != nil {
			t.Fatal(err)
		}
		reset, attrs := rearmBackoff(ctx, pool, nil, discoveryTestRun, time.Now().UTC())
		if reset != 10*time.Second || attrs != nil {
			t.Fatalf("after a unit end got countdown=%s attrs=%v want the 10s base and no back-off fields", reset, attrs)
		}
	})
}

// TestRearmBackoffFallsBackToTheBaseWhenTheReadFails pins the fail-open rule:
// a failed progress read never delays a run and logs a Warn.
func TestRearmBackoffFallsBackToTheBaseWhenTheReadFails(t *testing.T) {
	t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "10")
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		got, attrs := rearmBackoff(cancelled, pool, synclog.Default(), discoveryTestRun, time.Now().UTC())
		if got != 10*time.Second || attrs != nil {
			t.Fatalf("got countdown=%s attrs=%v want the base and no back-off fields on a read error", got, attrs)
		}
	})
}
