//go:build integration

package syncdispatchruntime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// throughputPass is one dispatch pass the relay stand-in delivered: when its
// wakeup was due, when it started, and how many units it put in flight.
type throughputPass struct {
	dueAt     time.Time
	startedAt time.Time
	claimed   int
}

// relayDueDispatchWakeup stands in for the reconciler relay: it delivers the
// run's dispatch_sync_run row once it is pending and due, leaving it in the
// 'dispatched' state the transport fence at the top of Dispatch requires.
func relayDueDispatchWakeup(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (string, time.Time, bool) {
	t.Helper()
	var outboxID string
	var dueAt time.Time
	err := pool.QueryRow(ctx, `
UPDATE sync_dispatch_outbox
SET status = 'dispatched', dispatched_at = now(), dispatched_transport = 'river',
    dispatched_route_generation = $2, updated_at = now()
WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run'
  AND status = 'pending' AND claim_token IS NULL AND available_at <= now()
RETURNING id::text, available_at`, discoveryTestRun, dispatchRouteGeneration).Scan(&outboxID, &dueAt)
	if err != nil {
		return "", time.Time{}, false
	}
	return outboxID, dueAt, true
}

func unitIDsInStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, status string) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT id::text FROM sync_run_units WHERE sync_run_id = $1 AND status = $2 ORDER BY id`, discoveryTestRun, status)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

// leaseUnitForCompletion moves a dispatched unit to 'running' under a fresh
// lease, as providersync's claim does, and returns the Claim its Complete
// needs.
func leaseUnitForCompletion(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unitID string) providersync.Claim {
	t.Helper()
	owner := uuid.NewString()
	leaseExpiresAt := time.Now().UTC().Add(time.Minute)
	var claim providersync.Claim
	if err := pool.QueryRow(ctx, `
UPDATE sync_run_units
SET status = 'running', attempts = attempts + 1, lease_owner = $2,
    lease_expires_at = $3, last_heartbeat_at = now(), updated_at = now()
WHERE id = $1::uuid AND status = 'dispatching'
RETURNING id::text, sync_run_id::text, org_id, integration_id::text, source_id::text, attempts`,
		unitID, owner, leaseExpiresAt).Scan(&claim.ID, &claim.SyncRunID, &claim.OrgID, &claim.IntegrationID, &claim.SourceID, &claim.Attempt); err != nil {
		t.Fatalf("lease %s: %v", unitID, err)
	}
	claim.SourceExternalID = claim.SourceID
	claim.Provider, claim.Dataset, claim.CostClass, claim.Mode = "github", "commits", providersync.CostMedium, "incremental"
	claim.CredentialID = uuid.NewString()
	claim.Owner, claim.LeaseExpiresAt = owner, leaseExpiresAt
	if err := claim.Validate(); err != nil {
		t.Fatalf("stand-in claim for %s is not a valid claim: %v", unitID, err)
	}
	return claim
}

// TestMediumBucketRunDrainsByUnitCompletionNotByCountdown pins CHAOS-10013 at
// the throughput the defect was measured in: N units of ONE medium bucket (cap
// 2) with the 60 s countdown must finish in far less than N/2 countdowns.
//
// The loop is the production pipeline with the two process boundaries stood
// in: the relay delivers the run's dispatch row when it is due, Dispatch is the
// real service, and each unit the pass puts in flight is claimed and completed
// through the REAL providersync.PostgresRepository, whose commit is what wakes
// dispatch. Nothing in the loop arms a wakeup itself.
//
// It asserts the MECHANISM, not only the outcome: every pass after the first
// must have been due at a unit-completion instant, never at a countdown; each
// pass must stay inside the cap (this is not a budget change); and the pass
// count must be the minimum the cap allows, plus at most one.
//
// RED CONTROL: remove the wakeDispatch call from providersync's Complete and
// the run stalls after the first pass behind the 60 s re-arm; the loop's 40 s
// deadline fails it naming the completed count and the pending wakeup's
// instant.
func TestMediumBucketRunDrainsByUnitCompletionNotByCountdown(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "60")
		const units = 6
		const mediumCap = 2
		for index := 0; index < units; index++ {
			if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github','commits',$4,'planned',now(),(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'medium','incremental',0,now())`,
				fmt.Sprintf("00000000-0000-4000-8000-0000000010%02d", index), discoveryTestOrg, discoveryTestRun, dispatchTestSource); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_outbox SET status = 'pending', available_at = now(),
	dispatched_transport = NULL, dispatched_route_generation = NULL WHERE id = $1`, dispatchServiceTestOutbox); err != nil {
			t.Fatal(err)
		}

		service := newTestDispatchService(t, pool)
		repository, err := providersync.NewPostgresRepository(pool)
		if err != nil {
			t.Fatal(err)
		}
		defer repository.Close()

		armedBefore := syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathProviderUnit, syncrunrollup.DispatchWakeOutcomeArmed)
		started := time.Now()
		deadline := started.Add(40 * time.Second)
		var passes []throughputPass
		var lastCompletion time.Time
		for len(unitIDsInStatus(t, ctx, pool, "success")) < units {
			if time.Now().After(deadline) {
				var pendingAt time.Time
				_ = pool.QueryRow(ctx, `SELECT available_at FROM sync_dispatch_outbox WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run'`, discoveryTestRun).Scan(&pendingAt)
				t.Fatalf("run did not drain in 40 s: %d of %d units success after %d passes; the next dispatch wakeup is due %s after the last completion -- "+
					"the run is waiting for the redispatch countdown, not woken by its units",
					len(unitIDsInStatus(t, ctx, pool, "success")), units, len(passes), pendingAt.Sub(lastCompletion).Round(time.Second))
			}
			outboxID, dueAt, due := relayDueDispatchWakeup(t, ctx, pool)
			if !due {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			before := len(unitIDsInStatus(t, ctx, pool, "dispatching"))
			pass := throughputPass{dueAt: dueAt, startedAt: time.Now()}
			args := dispatchTestArgs()
			args.DispatchOutbox = outboxID
			if err := service.Dispatch(ctx, args); err != nil {
				t.Fatalf("pass %d: Dispatch: %v", len(passes)+1, err)
			}
			inFlight := unitIDsInStatus(t, ctx, pool, "dispatching")
			pass.claimed = len(inFlight) - before
			passes = append(passes, pass)
			if len(inFlight) > mediumCap {
				t.Fatalf("pass %d left %d units in flight, want at most the medium cap %d", len(passes), len(inFlight), mediumCap)
			}
			// The worker stand-in: about one second of work in production, a
			// few milliseconds here. The lease is taken the way Claim takes it
			// (Claim itself needs a resolvable credential this harness does not
			// seed); the COMMIT is providersync's real Complete.
			for _, unitID := range inFlight {
				claim := leaseUnitForCompletion(t, ctx, pool, unitID)
				lastCompletion = time.Now().UTC()
				if err := repository.Complete(ctx, claim, map[string]any{"records": 1}, nil, lastCompletion.Add(-time.Millisecond), lastCompletion); err != nil {
					t.Fatalf("complete %s: %v", unitID, err)
				}
			}
		}
		elapsed := time.Since(started)

		for index, pass := range passes[1:] {
			if wait := pass.startedAt.Sub(passes[index].startedAt); wait >= 30*time.Second {
				t.Errorf("pass %d started %s after pass %d: it waited for the countdown, not a unit completion", index+2, wait.Round(time.Second), index+1)
			}
			if pass.dueAt.After(passes[index].startedAt.Add(30 * time.Second)) {
				t.Errorf("pass %d was due at %s, a countdown after the previous pass, want the instant a unit completed", index+2, pass.dueAt)
			}
		}
		minimum := (units + mediumCap - 1) / mediumCap
		if len(passes) < minimum || len(passes) > minimum+1 {
			t.Errorf("run drained in %d passes, want %d (cap %d of %d units) or one more", len(passes), minimum, mediumCap, units)
		}
		if armed := syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathProviderUnit, syncrunrollup.DispatchWakeOutcomeArmed) - armedBefore; armed < uint64(minimum-1) {
			t.Errorf("armed wake counter moved %d, want at least %d: a pass after the first must be bought by a wake", armed, minimum-1)
		}
		t.Logf("drained %d medium units in %s over %d passes (60 s countdown; the pre-fix pacing needs about %d countdowns)", units, elapsed.Round(time.Millisecond), len(passes), minimum-1)
	})
}

// TestScheduleRedispatchKeepsAUnitWakeThatLandedMidPass pins the second half of
// CHAOS-10013 fix (A). A unit of an EARLIER pass that completes while the
// current pass runs arms the run's row to 'pending' at its completion instant;
// the current pass then re-arms for its capped units at now + countdown. That
// re-arm must keep the earlier wake, or the bucket sits idle for the whole
// countdown with a free slot.
//
// RED CONTROL: restore scheduleRedispatch's unconditional overwrite (drop the
// rearmedByOther check) and available_at moves to now+60s.
func TestScheduleRedispatchKeepsAUnitWakeThatLandedMidPass(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ('00000000-0000-4000-8000-000000001100',$1,$2,'github','commits',$3,'planned',now(),(SELECT integration_id FROM sync_runs WHERE id = $2::uuid),'medium','incremental',0,now())`,
			discoveryTestOrg, discoveryTestRun, dispatchTestSource); err != nil {
			t.Fatal(err)
		}
		completedAt := pgNow()
		if !syncrunrollup.WakeDispatch(ctx, pool, discoveryTestRun, completedAt, syncrunrollup.DispatchWakePathProviderUnit) {
			t.Fatal("precondition: the unit wake did not arm (a planned unit waits)")
		}
		// The wake merges into the pass's own delivered row by LEAST, so it is
		// due at the earlier of that row's armed instant and the completion:
		// either way due now, not a countdown away.
		woken := dispatchOutboxAvailableAt(t, ctx, pool, discoveryTestRun)
		if woken.After(completedAt) {
			t.Fatalf("precondition: wake available_at=%s is after the completion %s", woken, completedAt)
		}
		scheduleRedispatch(ctx, pool, nil, discoveryTestRun, nil, completedAt.Add(200*time.Millisecond))
		if got := dispatchOutboxAvailableAt(t, ctx, pool, discoveryTestRun); !got.Equal(woken) {
			t.Fatalf("available_at=%s want=%s: the pass's countdown re-arm overwrote a unit wake that landed while it ran", got, woken)
		}
	})
}

// TestScheduleRedispatchStillCountsDownFromItsOwnDeliveredRow pins why the
// overwrite exists at all: the pass's OWN row is 'dispatched' with the past
// instant it was armed for, and LEAST alone would keep that instant and wake the
// run on the very next relay tick.
//
// RED CONTROL: skip the overwrite unconditionally and available_at stays at the
// stale past instant.
func TestScheduleRedispatchStillCountsDownFromItsOwnDeliveredRow(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		stale := pgNow().Add(-10 * time.Minute)
		if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_outbox SET available_at = $2 WHERE id = $1`, dispatchServiceTestOutbox, stale); err != nil {
			t.Fatal(err)
		}
		now := pgNow()
		scheduleRedispatch(ctx, pool, nil, discoveryTestRun, nil, now)
		got := dispatchOutboxAvailableAt(t, ctx, pool, discoveryTestRun)
		if want := now.Add(60 * time.Second); !got.Equal(want) {
			t.Fatalf("available_at=%s want=%s: a re-arm of the pass's own delivered row must count down, not keep the stale %s", got, want, stale)
		}
	})
}
