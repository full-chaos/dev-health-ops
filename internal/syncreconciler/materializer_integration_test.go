//go:build integration

package syncreconciler

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	materializerDispatchMissing  = "00000000-0000-4000-8000-000000004101"
	materializerExpiredClaim     = "00000000-0000-4000-8000-000000004102"
	materializerLiveClaim        = "00000000-0000-4000-8000-000000004103"
	materializerTerminalDenial   = "00000000-0000-4000-8000-000000004104"
	materializerFinalize         = "00000000-0000-4000-8000-000000004105"
	materializerDiscovery        = "00000000-0000-4000-8000-000000004106"
	materializerPostSyncMissing  = "00000000-0000-4000-8000-000000004107"
	materializerPostSyncExists   = "00000000-0000-4000-8000-000000004108"
	materializerRiverQueued      = "00000000-0000-4000-8000-000000004109"
	materializerStaleDispatch    = "00000000-0000-4000-8000-00000000410a"
	materializerFreshDispatch    = "00000000-0000-4000-8000-00000000410b"
	materializerRetryingDispatch = "00000000-0000-4000-8000-00000000410c"
	materializerFeatureDisabled  = "00000000-0000-4000-8000-00000000410d"
	materializerDiscoveryRetry   = "00000000-0000-4000-8000-00000000410e"
	// CHAOS-4359: the three run identities for the dispatch materializer's
	// 'planned' gap and the finalize refusal that must survive it.
	materializerPlannedDispatch  = "00000000-0000-4000-8000-00000000410f"
	materializerFreshPlanned     = "00000000-0000-4000-8000-000000004110"
	materializerStaleGenFinalize = "00000000-0000-4000-8000-000000004111"
)

func TestMaterializerRedispatchesStaleUnitsExactlyOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := createMaterializerIntegrationFixture(ctx, pool); err != nil {
		t.Fatal(err)
	}
	materializer, err := NewMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("stale dispatching River row re-arms once", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 24, 1, 0, 0, 0, time.UTC)
		cutoff := now.Add(-15 * time.Minute)
		seedRun(t, ctx, pool, materializerStaleDispatch, "running", now.Add(-2*time.Hour))
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004301",
			materializerStaleDispatch, "dispatching", nil, now.Add(-time.Hour))
		seedMaterializerDispatchedOutbox(t, ctx, pool, materializerStaleDispatch, "river-stale-job", now.Add(-2*time.Hour))

		result, err := materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 1 {
			t.Fatalf("stale dispatching graph did not rearm exactly once: %#v", result)
		}
		assertMaterializerDispatchState(t, ctx, pool, materializerStaleDispatch, "pending", nil, 1)

		result, err = materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 0 {
			t.Fatalf("stale dispatching graph amplified on second pass: %#v", result)
		}
		assertMaterializerDispatchState(t, ctx, pool, materializerStaleDispatch, "pending", nil, 1)
	})

	t.Run("fresh dispatching River row stays protected", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 24, 2, 0, 0, 0, time.UTC)
		cutoff := now.Add(-15 * time.Minute)
		seedRun(t, ctx, pool, materializerFreshDispatch, "running", now.Add(-2*time.Hour))
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004302",
			materializerFreshDispatch, "dispatching", nil, now.Add(-5*time.Minute))
		seedMaterializerDispatchedOutbox(t, ctx, pool, materializerFreshDispatch, "river-fresh-job", now.Add(-2*time.Hour))

		result, err := materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 0 {
			t.Fatalf("fresh dispatching graph rearmed unexpectedly: %#v", result)
		}
		assertMaterializerDispatchState(t, ctx, pool, materializerFreshDispatch, "dispatched", ptrString("river"), 1)
	})

	t.Run("retrying River row re-arms once when due", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 24, 3, 0, 0, 0, time.UTC)
		cutoff := now.Add(-15 * time.Minute)
		seedRun(t, ctx, pool, materializerRetryingDispatch, "running", now.Add(-2*time.Hour))
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004303",
			materializerRetryingDispatch, "retrying", ptrTime(now.Add(-time.Minute)), now.Add(-5*time.Minute))
		seedMaterializerDispatchedOutbox(t, ctx, pool, materializerRetryingDispatch, "river-retry-job", now.Add(-2*time.Hour))

		result, err := materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 1 {
			t.Fatalf("retrying graph did not rearm exactly once: %#v", result)
		}
		assertMaterializerDispatchState(t, ctx, pool, materializerRetryingDispatch, "pending", nil, 1)

		result, err = materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 0 {
			t.Fatalf("retrying graph amplified on second pass: %#v", result)
		}
		assertMaterializerDispatchState(t, ctx, pool, materializerRetryingDispatch, "pending", nil, 1)
	})

	t.Run("feature-disabled River row stays protected", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 24, 4, 0, 0, 0, time.UTC)
		cutoff := now.Add(-15 * time.Minute)
		seedRun(t, ctx, pool, materializerFeatureDisabled, "running", now.Add(-2*time.Hour))
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004304",
			materializerFeatureDisabled, "dispatching", nil, now.Add(-time.Hour))
		seedMaterializerFeatureDisabledOutbox(t, ctx, pool, materializerFeatureDisabled, now.Add(-2*time.Hour))

		result, err := materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 0 {
			t.Fatalf("feature-disabled row rearmed unexpectedly: %#v", result)
		}
		assertMaterializerDispatchState(t, ctx, pool, materializerFeatureDisabled, "dispatched", ptrString("river"), 1)
	})

	// CHAOS-4357: reproduces the live prod/local state verbatim -- a
	// reference-discovery ledger that went back to 'retrying' with a past
	// available_at, whose sync_dispatch_outbox row is still sitting
	// 'dispatched' from its PRIOR (now-stale) attempt with every claim field
	// NULL (the normal shape markRiverDispatchedSQL leaves behind -- NOT the
	// live/still-claimed shape "feature-disabled row stays protected" above
	// covers). Read-only queries against the affected local stack
	// (org 70d529e0, run c5b61360-...) showed exactly this row shape stuck
	// for 25+ minutes across multiple healthy reconciler ticks. This proves
	// whether Materializer.Step alone re-arms it.
	t.Run("retrying reference discovery past available_at re-arms its stale dispatched outbox row", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 24, 5, 0, 0, 0, time.UTC)
		cutoff := now.Add(-15 * time.Minute)
		seedRun(t, ctx, pool, materializerDiscoveryRetry, "planned", now.Add(-2*time.Hour))
		seedDiscoveryLedger(t, ctx, pool, materializerDiscoveryRetry, "retrying",
			now.Add(-time.Minute), nil, 1, ptrString("Reference discovery failed"))
		seedMaterializerDiscoveryDispatchedOutbox(t, ctx, pool, materializerDiscoveryRetry,
			"discovery-river-job", now.Add(-30*time.Minute), 3)

		result, err := materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Discovery != 1 {
			t.Fatalf("retrying discovery graph did not rearm exactly once: %#v", result)
		}
		// CHAOS-4357 round 2 (codex P2): DiscoveryRearmed is the narrow
		// stale-dispatched-recovery count the sync_dispatch_discovery_rearmed_total
		// metric publishes -- this row IS that exact case, so both counts
		// agree here (they diverge only for a fresh insert or a non-river
		// dispatched row, neither of which this subtest seeds).
		if result.DiscoveryRearmed != 1 {
			t.Fatalf("retrying discovery graph did not count as a stale-dispatched recovery: %#v", result)
		}
		// Materializer.Step alone re-arms status only -- attempts is the
		// KERNEL's claim counter (claimRiverRoutesSQL), so it stays whatever
		// it was (3, from the prior stale attempt) until something actually
		// claims this row again.
		assertMaterializerDiscoveryOutboxState(t, ctx, pool, materializerDiscoveryRetry, "pending", 3)

		result, err = materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Discovery != 0 {
			t.Fatalf("retrying discovery graph amplified on second pass: %#v", result)
		}
		if result.DiscoveryRearmed != 0 {
			t.Fatalf("retrying discovery graph recounted a recovery on second pass: %#v", result)
		}
		assertMaterializerDiscoveryOutboxState(t, ctx, pool, materializerDiscoveryRetry, "pending", 3)
	})

	// CHAOS-4357 round 2 (codex P2): a fresh discovery ledger with NO prior
	// outbox row at all is a real, ordinary materialization (Discovery
	// counts it), but it is NOT a "recovered a stranded dispatched row"
	// event -- nothing was ever dispatched, let alone stranded. Before this
	// fix DiscoveryRearmed didn't exist and the metric was sourced straight
	// from Discovery, so this exact case would have inflated
	// sync_dispatch_discovery_rearmed_total with a false recovery signal.
	t.Run("a fresh discovery materialization is not counted as a recovery", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
		cutoff := now.Add(-15 * time.Minute)
		seedRun(t, ctx, pool, materializerDiscoveryRetry, "planned", now.Add(-2*time.Hour))
		seedDiscoveryLedger(t, ctx, pool, materializerDiscoveryRetry, "planned",
			now.Add(-time.Minute), nil, 0, nil)
		// Deliberately no seedMaterializerDiscoveryDispatchedOutbox call --
		// this run has never had an outbox row of any kind.

		result, err := materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Discovery != 1 {
			t.Fatalf("fresh discovery graph did not materialize exactly once: %#v", result)
		}
		if result.DiscoveryRearmed != 0 {
			t.Fatalf("fresh discovery materialization miscounted as a stale-dispatched recovery: %#v", result)
		}
		assertMaterializerDiscoveryOutboxState(t, ctx, pool, materializerDiscoveryRetry, "pending", 0)
	})

	// CHAOS-4357 round 2 (codex P1): a FRESH river delivery -- dispatched
	// well within the staleDispatchCutoff grace window -- must NOT be
	// re-armed even though the ledger's available_at already reads as due
	// (it was due the moment the retry was published). Before this fix, the
	// very next materializer tick would reset this row to 'pending' and the
	// kernel would publish a SECOND River job for the same retry, repeating
	// every tick until the first job finally executed -- amplifying one
	// retry into an unbounded stream of duplicate jobs.
	t.Run("a freshly dispatched retry within the grace window is not amplified", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 24, 7, 0, 0, 0, time.UTC)
		cutoff := now.Add(-15 * time.Minute)
		seedRun(t, ctx, pool, materializerDiscoveryRetry, "planned", now.Add(-2*time.Hour))
		seedDiscoveryLedger(t, ctx, pool, materializerDiscoveryRetry, "retrying",
			now.Add(-time.Minute), nil, 1, ptrString("Reference discovery failed"))
		// Dispatched 5 minutes ago -- well after cutoff (15 minutes ago), so
		// still within its grace period to actually be claimed and start.
		seedMaterializerDiscoveryDispatchedOutbox(t, ctx, pool, materializerDiscoveryRetry,
			"discovery-fresh-job", now.Add(-5*time.Minute), 1)

		result, err := materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Discovery != 0 {
			t.Fatalf("fresh in-flight delivery was touched: %#v", result)
		}
		if result.DiscoveryRearmed != 0 {
			t.Fatalf("fresh in-flight delivery was counted as recovered: %#v", result)
		}
		assertMaterializerDiscoveryOutboxState(t, ctx, pool, materializerDiscoveryRetry, "dispatched", 1)
	})

	// CHAOS-4359 (1): reproduces live local-stack run
	// 837d069b-09af-5661-bf10-97b09addbd40 (org 70d529e0) verbatim -- eight
	// units still 'planned' with attempts 0 beside 'success' and 'failed'
	// siblings, and a dispatch_sync_run outbox row stuck 'dispatched' via
	// 'river' since 2026-08-29 02:16, attempts unchanged at 9 across eleven
	// days of healthy reconciler ticks. The candidate CTE selects this run
	// (unit.status = 'planned' is its first disjunct) but the UPDATE guard's
	// EXISTS re-check re-confirms only the 'dispatching' and 'retrying'
	// disjuncts, so the row is chosen and then silently refused: no other
	// path resets a dispatch_sync_run delivery, and TerminalDeliveryRepair's
	// three River-terminal branches need a discarded/cancelled job, which a
	// delivery that COMPLETED does not have. The run never dispatches again.
	t.Run("planned units behind a stale River dispatch delivery re-arm once", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 24, 8, 0, 0, 0, time.UTC)
		cutoff := now.Add(-15 * time.Minute)
		stranded := now.Add(-11 * 24 * time.Hour)
		seedRun(t, ctx, pool, materializerPlannedDispatch, "dispatching", stranded)
		// The live mix, not a single-unit reduction: a 'planned' unit is the
		// only reason this run is a candidate, and the terminal siblings are
		// what make it look finished to every other repair.
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004305",
			materializerPlannedDispatch, "planned", nil, stranded)
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004306",
			materializerPlannedDispatch, "success", nil, stranded)
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004307",
			materializerPlannedDispatch, "failed", nil, stranded)
		seedMaterializerDispatchedOutbox(t, ctx, pool, materializerPlannedDispatch,
			"river-planned-job", stranded)

		result, err := materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 1 {
			t.Fatalf("planned-unit graph did not rearm exactly once: %#v", result)
		}
		assertMaterializerDispatchState(t, ctx, pool, materializerPlannedDispatch, "pending", nil, 1)

		result, err = materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 0 {
			t.Fatalf("planned-unit graph amplified on second pass: %#v", result)
		}
		assertMaterializerDispatchState(t, ctx, pool, materializerPlannedDispatch, "pending", nil, 1)
	})

	// CHAOS-4359 (1), the other half of the same clause: a 'planned' unit
	// says NOTHING about how long the current delivery has had to run,
	// because a unit stays 'planned' for the whole window between publishing
	// dispatch_sync_run and that job claiming its units. Admitting 'planned'
	// without the delivery's OWN staleness gate would reset a freshly
	// published row on the very next one-second tick and make the kernel
	// publish a second dispatch job for the same run, every tick, until the
	// first one finally executed -- CHAOS-4357 round 2's P1 amplification,
	// reproduced on a different kind. The grace window
	// (SYNC_UNIT_DISPATCH_STALE_SECONDS, $2) is what stops it.
	t.Run("a freshly dispatched planned-unit delivery within the grace window is not amplified", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 24, 9, 0, 0, 0, time.UTC)
		cutoff := now.Add(-15 * time.Minute)
		seedRun(t, ctx, pool, materializerFreshPlanned, "dispatching", now.Add(-2*time.Hour))
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004308",
			materializerFreshPlanned, "planned", nil, now.Add(-5*time.Minute))
		// Dispatched 5 minutes ago -- well after the 15-minute cutoff, so
		// still inside its grace period to be claimed and start executing.
		seedMaterializerDispatchedOutbox(t, ctx, pool, materializerFreshPlanned,
			"river-fresh-planned-job", now.Add(-5*time.Minute))

		result, err := materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 0 {
			t.Fatalf("fresh in-flight planned-unit delivery was touched: %#v", result)
		}
		assertMaterializerDispatchState(t, ctx, pool, materializerFreshPlanned,
			"dispatched", ptrString("river"), 1)
	})

	// CHAOS-5462 / CHAOS-4359 (2): the materializer's finalize guard refuses
	// EVERY River-dispatched finalize row, and that refusal is deliberate,
	// not the same omission as the dispatch gap above. The materializer runs
	// on the coordinator role: it cannot read river_job, so it has no
	// evidence a River finalize delivery is dead, and it does not read
	// sync_dispatch_transport_routes, so it has no route-generation fence
	// either. Re-arming from domain readiness alone would therefore both
	// double-deliver a live finalize and resurrect stale-generation history.
	//
	// The row below is live local-stack shape: sync_run
	// aedd0504-ad9f-5d71-8b25-b03f0ce4665f's finalize delivery, dispatched
	// 2026-08-28 03:41 at route generation 4 while the current
	// finalize_sync_run route is river generation 2 -- and its run is
	// finalize-READY by every clause of finalizeReadyRunPredicate, so the
	// candidate CTE does select it. Only this guard keeps it out.
	// CHAOS-5456's TerminalDeliveryRepair backstop is what may re-arm this
	// class, queue-side, and only once generation, route, claim and River
	// liveness all hold. This subtest is the fence: it fails the moment a
	// change to materializeFinalizeSQL lets the coordinator re-arm one.
	t.Run("a River-dispatched finalize row is never re-armed by the materializer", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 24, 10, 0, 0, 0, time.UTC)
		cutoff := now.Add(-15 * time.Minute)
		stranded := now.Add(-12 * 24 * time.Hour)
		seedRun(t, ctx, pool, materializerStaleGenFinalize, "dispatching", stranded)
		// Every unit terminal and no discovery ledger: finalizeReadyRunPredicate
		// holds, so readiness is NOT what excludes this row.
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004309",
			materializerStaleGenFinalize, "success", nil, stranded)
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-00000000430a",
			materializerStaleGenFinalize, "failed", nil, stranded)
		seedMaterializerFinalizeDispatchedOutbox(t, ctx, pool, materializerStaleGenFinalize,
			"river-stale-generation-finalize-job", stranded, 4)

		assertMaterializerFinalizeIsReady(t, ctx, pool, materializerStaleGenFinalize, true)

		result, err := materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Finalize != 0 {
			t.Fatalf("stale-generation River finalize row was re-armed: %#v", result)
		}
		assertMaterializerFinalizeState(t, ctx, pool, materializerStaleGenFinalize,
			"dispatched", ptrString("river"), 4)
	})
}

func TestMaterializerPostgresConcurrencyAndRollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := createMaterializerIntegrationFixture(ctx, pool); err != nil {
		t.Fatal(err)
	}
	materializer, err := NewMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("two replicas converge without changing delivery state", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 23, 20, 0, 0, 0, time.UTC)
		seedMaterializerIntegrationGraph(t, ctx, pool, now)

		start := make(chan struct{})
		results := make(chan error, 2)
		var ready sync.WaitGroup
		ready.Add(2)
		for replica := 0; replica < 2; replica++ {
			go func() {
				ready.Done()
				<-start
				_, stepErr := materializer.Step(ctx, now, now.Add(-15*time.Minute), 20)
				results <- stepErr
			}()
		}
		ready.Wait()
		close(start)
		for replica := 0; replica < 2; replica++ {
			if err := <-results; err != nil {
				t.Fatalf("replica %d Step(): %v", replica, err)
			}
		}

		assertMaterializerOutboxCount(t, ctx, pool, materializerDispatchMissing, "dispatch_sync_run", 1)
		assertMaterializerOutboxCount(t, ctx, pool, materializerExpiredClaim, "dispatch_sync_run", 1)
		assertMaterializerOutboxCount(t, ctx, pool, materializerLiveClaim, "dispatch_sync_run", 1)
		assertMaterializerOutboxCount(t, ctx, pool, materializerTerminalDenial, "dispatch_sync_run", 1)
		assertMaterializerOutboxCount(t, ctx, pool, materializerFinalize, "finalize_sync_run", 1)
		assertMaterializerOutboxCount(t, ctx, pool, materializerDiscovery, "reference_discovery", 1)
		assertMaterializerOutboxCount(t, ctx, pool, materializerRiverQueued, "reference_discovery", 1)
		assertMaterializerOutboxCount(t, ctx, pool, materializerPostSyncMissing, "post_sync", 1)
		assertMaterializerOutboxCount(t, ctx, pool, materializerPostSyncExists, "post_sync", 1)

		var (
			status         string
			availableAt    time.Time
			updatedAt      time.Time
			claimToken     *string
			claimExpiresAt *time.Time
			attempts       int
		)
		if err := pool.QueryRow(ctx, `
			SELECT status, available_at, updated_at, claim_token, claim_expires_at, attempts
			FROM public.sync_dispatch_outbox
			WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run'`,
			materializerExpiredClaim,
		).Scan(&status, &availableAt, &updatedAt, &claimToken, &claimExpiresAt, &attempts); err != nil {
			t.Fatal(err)
		}
		if status != "pending" || claimToken == nil || *claimToken != "expired-claim" ||
			claimExpiresAt == nil || !claimExpiresAt.Equal(now.Add(-time.Minute)) ||
			!availableAt.Equal(now.Add(-2*time.Hour)) || !updatedAt.Equal(now.Add(-2*time.Hour)) ||
			attempts != 4 {
			t.Fatalf("expired claim = status:%s claim:%v/%v attempts:%d available:%s updated:%s",
				status, claimToken, claimExpiresAt, attempts, availableAt, updatedAt)
		}

		var liveToken, liveTransport *string
		var liveExpiry *time.Time
		var liveGeneration *int64
		if err := pool.QueryRow(ctx, `
			SELECT claim_token, claim_expires_at, claim_transport, claim_route_generation
			FROM public.sync_dispatch_outbox
			WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run'`,
			materializerLiveClaim,
		).Scan(&liveToken, &liveExpiry, &liveTransport, &liveGeneration); err != nil {
			t.Fatal(err)
		}
		if liveToken == nil || *liveToken != "live-claim" || liveExpiry == nil || !liveExpiry.After(now) ||
			liveTransport == nil || *liveTransport != "celery" || liveGeneration == nil || *liveGeneration != 9 {
			t.Fatalf("live claim was not preserved: %v/%v/%v/%v", liveToken, liveExpiry, liveTransport, liveGeneration)
		}

		var denialStatus, denialError string
		var denialDispatchedAt time.Time
		if err := pool.QueryRow(ctx, `
			SELECT status, last_error, dispatched_at
			FROM public.sync_dispatch_outbox
			WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run'`,
			materializerTerminalDenial,
		).Scan(&denialStatus, &denialError, &denialDispatchedAt); err != nil {
			t.Fatal(err)
		}
		if denialStatus != "dispatched" || denialError != "feature_disabled" ||
			!denialDispatchedAt.Equal(now.Add(-2*time.Hour)) {
			t.Fatalf("terminal denial changed: %s/%s/%s", denialStatus, denialError, denialDispatchedAt)
		}

		var discoveryStatus string
		var discoveryDispatchedAt *time.Time
		var discoveryTransport *string
		var discoveryClaimToken, discoveryClaimTransport *string
		var discoveryClaimExpiry *time.Time
		var discoveryClaimGeneration *int64
		if err := pool.QueryRow(ctx, `
			SELECT status, dispatched_at, dispatched_transport,
				claim_token, claim_expires_at, claim_transport, claim_route_generation
			FROM public.sync_dispatch_outbox
			WHERE sync_run_id = $1 AND kind = 'reference_discovery'`,
			materializerDiscovery,
		).Scan(
			&discoveryStatus, &discoveryDispatchedAt, &discoveryTransport,
			&discoveryClaimToken, &discoveryClaimExpiry,
			&discoveryClaimTransport, &discoveryClaimGeneration,
		); err != nil {
			t.Fatal(err)
		}
		if discoveryStatus != "pending" || discoveryDispatchedAt != nil || discoveryTransport != nil ||
			discoveryClaimToken == nil || *discoveryClaimToken != "discovery-live" ||
			discoveryClaimExpiry == nil || !discoveryClaimExpiry.Equal(now.Add(time.Hour)) ||
			discoveryClaimTransport == nil || *discoveryClaimTransport != "celery" ||
			discoveryClaimGeneration == nil || *discoveryClaimGeneration != 3 {
			t.Fatalf("ordinary dispatched discovery was not rearmed with its live claim: %s/%v/%v/%v/%v/%v/%v",
				discoveryStatus, discoveryDispatchedAt, discoveryTransport,
				discoveryClaimToken, discoveryClaimExpiry,
				discoveryClaimTransport, discoveryClaimGeneration)
		}

		// The finalize row stays the "delivery state unchanged" witness: the
		// materializer refuses every River-dispatched finalize delivery by
		// design, whatever its age, because it can see neither River job
		// state nor the route generation (see materializeFinalizeSQL's own
		// comment, and CHAOS-5462).
		{
			var riverStatus string
			var riverTransport, riverJobID *string
			var riverAttempts int
			if err := pool.QueryRow(ctx, `
				SELECT status, dispatched_transport, transport_job_id, attempts
				FROM public.sync_dispatch_outbox
				WHERE sync_run_id = $1 AND kind = 'finalize_sync_run'`,
				materializerFinalize,
			).Scan(&riverStatus, &riverTransport, &riverJobID, &riverAttempts); err != nil {
				t.Fatal(err)
			}
			if riverStatus != "dispatched" || riverTransport == nil || *riverTransport != "river" ||
				riverJobID == nil || *riverJobID != "river-finalize-queued" || riverAttempts != 1 {
				t.Fatalf("queued River finalize_sync_run delivery was rearmed: %s/%v/%v/%d",
					riverStatus, riverTransport, riverJobID, riverAttempts)
			}
		}

		// CHAOS-4359: materializerDispatchMissing is the stranded dispatch
		// shape -- a 'planned' unit behind a River delivery dispatched two
		// hours ago, well past this Step's fifteen-minute grace window. It
		// USED to be asserted here as a second "unchanged" witness, which
		// encoded the very omission CHAOS-4359 reports: the candidate CTE
		// selects it and the UPDATE guard then refused it forever. It now
		// re-arms, and this is the stronger property for a concurrency test
		// to hold: TWO replicas racing the same statement re-arm it EXACTLY
		// ONCE, to 'pending' with every delivery field cleared, never twice
		// and never into a torn half-cleared row. The unique
		// (sync_run_id, kind) key is what arbitrates, and
		// assertMaterializerOutboxCount above proves no second row appeared.
		assertMaterializerDispatchState(t, ctx, pool, materializerDispatchMissing, "pending", nil, 1)
		{
			var jobID *string
			var dispatchedAt *time.Time
			var generation *int64
			if err := pool.QueryRow(ctx, `
				SELECT transport_job_id, dispatched_at, dispatched_route_generation
				FROM public.sync_dispatch_outbox
				WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run'`,
				materializerDispatchMissing,
			).Scan(&jobID, &dispatchedAt, &generation); err != nil {
				t.Fatal(err)
			}
			if jobID != nil || dispatchedAt != nil || generation != nil {
				t.Fatalf("re-armed dispatch_sync_run kept delivery identity: %v/%v/%v",
					jobID, dispatchedAt, generation)
			}
		}

		// CHAOS-4357: materializerRiverQueued's ledger is seeded 'retrying'
		// with a past available_at (same seed as materializerDiscovery just
		// above) -- its outbox row being 'dispatched'+'river' does NOT mean a
		// live delivery is still in flight; it means a PRIOR delivery already
		// ran and the ledger has since proven it needs another attempt. Before
		// the fix this row -- like the real org-70d529e0 incident -- would
		// have stayed 'dispatched' forever, identical to every other kind
		// above, because nothing else re-arms a reference_discovery outbox
		// row once handleFailure's own direct upsert loses its race with an
		// early "not_claimed" River no-op. The ledger is the source of truth
		// here (unlike dispatch_sync_run/finalize_sync_run, it has no
		// separate live-claim signal of its own to fall back on), so it must
		// be re-armed, not left stranded.
		var riverQueuedStatus string
		var riverQueuedTransport, riverQueuedJobID *string
		var riverQueuedAttempts int
		if err := pool.QueryRow(ctx, `
			SELECT status, dispatched_transport, transport_job_id, attempts
			FROM public.sync_dispatch_outbox
			WHERE sync_run_id = $1 AND kind = 'reference_discovery'`,
			materializerRiverQueued,
		).Scan(&riverQueuedStatus, &riverQueuedTransport, &riverQueuedJobID, &riverQueuedAttempts); err != nil {
			t.Fatal(err)
		}
		if riverQueuedStatus != "pending" || riverQueuedTransport != nil || riverQueuedJobID != nil ||
			riverQueuedAttempts != 1 {
			t.Fatalf("stale reference_discovery delivery was not rearmed: %s/%v/%v/%d",
				riverQueuedStatus, riverQueuedTransport, riverQueuedJobID, riverQueuedAttempts)
		}

		var postStatus string
		var postUpdatedAt time.Time
		if err := pool.QueryRow(ctx, `
			SELECT status, updated_at
			FROM public.sync_dispatch_outbox
			WHERE sync_run_id = $1 AND kind = 'post_sync'`,
			materializerPostSyncExists,
		).Scan(&postStatus, &postUpdatedAt); err != nil {
			t.Fatal(err)
		}
		if postStatus != "dispatched" || !postUpdatedAt.Equal(now.Add(-2*time.Hour)) {
			t.Fatalf("existing post_sync row changed: %s/%s", postStatus, postUpdatedAt)
		}
		var missingPostOrg string
		if err := pool.QueryRow(ctx, `
			SELECT org_id
			FROM public.sync_dispatch_outbox
			WHERE sync_run_id = $1 AND kind = 'post_sync'`,
			materializerPostSyncMissing,
		).Scan(&missingPostOrg); err != nil {
			t.Fatal(err)
		}
		if missingPostOrg != "org-materializer" {
			t.Fatalf("missing post_sync used non-authoritative org_id %q", missingPostOrg)
		}

		var unexpectedKinds int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM public.sync_dispatch_outbox
			WHERE sync_run_id = $1 AND kind <> 'finalize_sync_run'`,
			materializerFinalize,
		).Scan(&unexpectedKinds); err != nil {
			t.Fatal(err)
		}
		if unexpectedKinds != 0 {
			t.Fatalf("finalizable run received %d non-finalize wakeups", unexpectedKinds)
		}
	})

	t.Run("later statement failure rolls back earlier materialization", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 23, 21, 0, 0, 0, time.UTC)
		seedRun(t, ctx, pool, materializerDispatchMissing, "running", now.Add(-2*time.Hour))
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004201",
			materializerDispatchMissing, "planned", nil, now.Add(-time.Hour))
		seedRun(t, ctx, pool, materializerFinalize, "running", now.Add(-time.Hour))
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004205",
			materializerFinalize, "success", nil, now.Add(-time.Minute))
		if _, err := pool.Exec(ctx, "INSERT INTO public.materializer_failures (kind) VALUES ('finalize_sync_run')"); err != nil {
			t.Fatal(err)
		}

		result, err := materializer.Step(ctx, now, now.Add(-15*time.Minute), 20)
		if !errors.Is(err, ErrUnavailable) || !reflect.DeepEqual(result, MaterializerResult{}) {
			t.Fatalf("failed Step() = %#v, %v", result, err)
		}
		var rows int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM public.sync_dispatch_outbox").Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 0 {
			t.Fatalf("failed transaction persisted %d earlier materializations", rows)
		}
	})

	t.Run("limit selects the deterministic first run only", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 23, 22, 0, 0, 0, time.UTC)
		for index, runID := range []string{materializerExpiredClaim, materializerDispatchMissing} {
			seedRun(t, ctx, pool, runID, "running", now.Add(-time.Duration(index+1)*time.Hour))
			seedUnit(t, ctx, pool,
				"00000000-0000-4000-8000-"+leftPadMaterializerID(4301+index),
				runID, "planned", nil, now.Add(-time.Hour))
		}

		result, err := materializer.Step(ctx, now, now.Add(-15*time.Minute), 1)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 1 {
			t.Fatalf("bounded Step() result = %#v", result)
		}
		assertMaterializerOutboxCount(t, ctx, pool, materializerDispatchMissing, "dispatch_sync_run", 1)
		assertMaterializerOutboxCount(t, ctx, pool, materializerExpiredClaim, "dispatch_sync_run", 0)
	})

	t.Run("scheduled graph waits for committed occurrence readiness", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 23, 23, 0, 0, 0, time.UTC)
		seedRun(t, ctx, pool, materializerDispatchMissing, "planned", now.Add(-time.Hour))
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004401",
			materializerDispatchMissing, "planned", nil, now.Add(-time.Hour))
		if _, err := pool.Exec(ctx, `UPDATE public.sync_runs SET triggered_by='schedule' WHERE id=$1`, materializerDispatchMissing); err != nil {
			t.Fatal(err)
		}

		result, err := materializer.Step(ctx, now, now.Add(-15*time.Minute), 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 0 || result.Finalize != 0 {
			t.Fatalf("unready scheduled graph materialized a wakeup: %#v", result)
		}
		assertMaterializerOutboxCount(t, ctx, pool, materializerDispatchMissing, "dispatch_sync_run", 0)

		if _, err := pool.Exec(ctx, `
			INSERT INTO public.scheduled_sync_occurrences
				(occurrence_id,sync_run_id,job_run_id,reconcile_status)
			VALUES ('ready-occurrence',$1,'00000000-0000-4000-8000-000000004499','completed')`, materializerDispatchMissing); err != nil {
			t.Fatal(err)
		}
		result, err = materializer.Step(ctx, now, now.Add(-15*time.Minute), 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 1 {
			t.Fatalf("ready scheduled graph did not materialize exactly one dispatch: %#v", result)
		}
		assertMaterializerOutboxCount(t, ctx, pool, materializerDispatchMissing, "dispatch_sync_run", 1)
	})

	t.Run("scheduled zero-unit graph cannot finalize before readiness", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 24, 0, 0, 0, 0, time.UTC)
		seedRun(t, ctx, pool, materializerFinalize, "planned", now.Add(-time.Hour))
		if _, err := pool.Exec(ctx, `UPDATE public.sync_runs SET triggered_by='schedule' WHERE id=$1`, materializerFinalize); err != nil {
			t.Fatal(err)
		}
		result, err := materializer.Step(ctx, now, now.Add(-15*time.Minute), 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Finalize != 0 {
			t.Fatalf("unready zero-unit scheduled graph finalized: %#v", result)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO public.scheduled_sync_occurrences
				(occurrence_id,sync_run_id,job_run_id,reconcile_status)
			VALUES ('ready-zero-occurrence',$1,'00000000-0000-4000-8000-000000004498','completed')`, materializerFinalize); err != nil {
			t.Fatal(err)
		}
		result, err = materializer.Step(ctx, now, now.Add(-15*time.Minute), 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Finalize != 1 {
			t.Fatalf("ready zero-unit scheduled graph finalize count=%d, want 1", result.Finalize)
		}
		assertMaterializerOutboxCount(t, ctx, pool, materializerFinalize, "finalize_sync_run", 1)
	})
}

func createMaterializerIntegrationFixture(ctx context.Context, pool *pgxpool.Pool) error {
	for _, statement := range []string{
		"CREATE EXTENSION IF NOT EXISTS pgcrypto",
		`CREATE TABLE public.sync_runs (
			id uuid PRIMARY KEY,
			org_id text NOT NULL,
			triggered_by text NOT NULL DEFAULT 'manual',
			status text NOT NULL,
			created_at timestamptz NOT NULL
		)`,
		`CREATE TABLE public.scheduled_sync_occurrences (
			occurrence_id text PRIMARY KEY,
			sync_run_id uuid,
			job_run_id uuid,
			reconcile_status text NOT NULL
		)`,
		`CREATE TABLE public.sync_run_units (
			id uuid PRIMARY KEY,
			sync_run_id uuid NOT NULL REFERENCES public.sync_runs(id),
			status text NOT NULL,
			available_at timestamptz,
			updated_at timestamptz NOT NULL
		)`,
		// CHAOS-4114: the maintained executed-proof projection. It is in
		// domainPosture's manifest, and the scheduler/worker write paths stamp
		// it inside the same transaction that writes sync_run_units, so a venue
		// without it fails those writes outright.
		`CREATE TABLE public.sync_executed_proof_ledger (
			provider text NOT NULL,
			dataset_key text NOT NULL,
			attempted_at timestamptz NOT NULL,
			proven_at timestamptz,
			PRIMARY KEY (provider, dataset_key),
			CONSTRAINT ck_sync_executed_proof_ledger_provider_normalized
				CHECK (provider = lower(provider) AND btrim(provider) <> ''),
			CONSTRAINT ck_sync_executed_proof_ledger_dataset_normalized
				CHECK (dataset_key = lower(dataset_key) AND btrim(dataset_key) <> '')
		)`,
		`CREATE TABLE public.sync_run_reference_discoveries (
			id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			sync_run_id uuid NOT NULL UNIQUE REFERENCES public.sync_runs(id),
			status text NOT NULL,
			available_at timestamptz NOT NULL,
			lease_expires_at timestamptz
		)`,
		`CREATE TABLE public.sync_run_post_dispatches (
			id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			org_id text NOT NULL,
			sync_run_id uuid NOT NULL REFERENCES public.sync_runs(id),
			kind text NOT NULL,
			dispatched_at timestamptz NOT NULL,
			UNIQUE (sync_run_id, kind)
		)`,
		`CREATE TABLE public.sync_dispatch_outbox (
			id uuid PRIMARY KEY,
			org_id text NOT NULL,
			sync_run_id uuid NOT NULL REFERENCES public.sync_runs(id),
			kind text NOT NULL,
			status text NOT NULL,
			available_at timestamptz NOT NULL,
			attempts integer NOT NULL,
			last_error text,
			dispatched_at timestamptz,
			claim_token text,
			claim_expires_at timestamptz,
			claim_transport text,
			claim_route_generation bigint,
			dispatched_transport text,
			dispatched_route_generation bigint,
			transport_job_id text,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL,
			UNIQUE (sync_run_id, kind)
		)`,
		"CREATE TABLE public.materializer_failures (kind text PRIMARY KEY)",
		`CREATE FUNCTION public.fail_materializer_insert() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF EXISTS (
				SELECT 1 FROM public.materializer_failures
				WHERE kind = NEW.kind
			) THEN
				RAISE EXCEPTION 'injected materializer failure for %', NEW.kind;
			END IF;
			RETURN NEW;
		END;
		$$`,
		`CREATE TRIGGER materializer_failure
		BEFORE INSERT OR UPDATE ON public.sync_dispatch_outbox
		FOR EACH ROW EXECUTE FUNCTION public.fail_materializer_insert()`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func resetMaterializerIntegrationTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, statement := range []string{
		"TRUNCATE public.materializer_failures",
		"TRUNCATE public.sync_dispatch_outbox",
		"TRUNCATE public.scheduled_sync_occurrences",
		"TRUNCATE public.sync_run_post_dispatches",
		"TRUNCATE public.sync_run_reference_discoveries",
		"TRUNCATE public.sync_run_units",
		"TRUNCATE public.sync_runs CASCADE",
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

func seedMaterializerIntegrationGraph(t *testing.T, ctx context.Context, pool *pgxpool.Pool, now time.Time) {
	t.Helper()
	for index, runID := range []string{
		materializerDispatchMissing,
		materializerExpiredClaim,
		materializerLiveClaim,
		materializerTerminalDenial,
	} {
		seedRun(t, ctx, pool, runID, "running", now.Add(-time.Duration(8-index)*time.Hour))
		seedUnit(t, ctx, pool,
			"00000000-0000-4000-8000-"+leftPadMaterializerID(4201+index),
			runID, "planned", nil, now.Add(-time.Hour))
	}
	seedRun(t, ctx, pool, materializerFinalize, "running", now.Add(-4*time.Hour))
	seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004205",
		materializerFinalize, "success", nil, now.Add(-time.Minute))
	for _, queued := range []struct {
		runID string
		kind  string
		jobID string
	}{
		{materializerDispatchMissing, "dispatch_sync_run", "river-dispatch-queued"},
		{materializerFinalize, "finalize_sync_run", "river-finalize-queued"},
	} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO public.sync_dispatch_outbox (
				id, org_id, sync_run_id, kind, status, available_at, attempts,
				dispatched_at, dispatched_transport, dispatched_route_generation,
				transport_job_id, created_at, updated_at
			) VALUES (
				gen_random_uuid(), 'org-materializer', $1, $2,
				'dispatched', $3, 1, $3, 'river', 2, $4, $3, $3
			)`, queued.runID, queued.kind, now.Add(-2*time.Hour), queued.jobID); err != nil {
			t.Fatal(err)
		}
	}

	seedRun(t, ctx, pool, materializerDiscovery, "running", now.Add(-3*time.Hour))
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_reference_discoveries (
			sync_run_id, status, available_at, lease_expires_at
		) VALUES ($1, 'retrying', $2, NULL)`, materializerDiscovery, now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	seedRun(t, ctx, pool, materializerRiverQueued, "running", now.Add(-3*time.Hour))
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_reference_discoveries (
			sync_run_id, status, available_at, lease_expires_at
		) VALUES ($1, 'retrying', $2, NULL)`, materializerRiverQueued, now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}

	for _, runID := range []string{materializerPostSyncMissing, materializerPostSyncExists} {
		seedRun(t, ctx, pool, runID, "success", now.Add(-2*time.Hour))
		if _, err := pool.Exec(ctx, `
			INSERT INTO public.sync_run_post_dispatches (
				org_id, sync_run_id, kind, dispatched_at
			) VALUES ($3, $1, 'post_sync', $2)`,
			runID, now.Add(-2*time.Hour),
			map[bool]string{true: "stale-ledger-org", false: "org-materializer"}[runID == materializerPostSyncMissing]); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			claim_token, claim_expires_at, claim_transport, claim_route_generation,
			created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'dispatch_sync_run',
			'pending', $2, 4, 'expired-claim', $3, 'celery', 8, $2, $2
		)`, materializerExpiredClaim, now.Add(-2*time.Hour), now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			claim_token, claim_expires_at, claim_transport, claim_route_generation,
			created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'dispatch_sync_run',
			'pending', $2, 5, 'live-claim', $3, 'celery', 9, $2, $2
		)`, materializerLiveClaim, now.Add(-2*time.Hour), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			last_error, dispatched_at, created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'dispatch_sync_run',
			'dispatched', $2, 1, 'feature_disabled', $2, $2, $2
		)`, materializerTerminalDenial, now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			last_error, dispatched_at, dispatched_transport, dispatched_route_generation,
			transport_job_id, claim_token, claim_expires_at, claim_transport,
			claim_route_generation, created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'reference_discovery',
			'dispatched', $2, 2, 'ordinary_failure', $2, 'celery', 3, 'celery-job',
			'discovery-live', $3, 'celery', 3, $2, $2
		)`, materializerDiscovery, now.Add(-2*time.Hour), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			dispatched_at, dispatched_transport, dispatched_route_generation,
			transport_job_id, created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'reference_discovery',
			'dispatched', $2, 1, $2, 'river', 2, 'river-discovery-queued', $2, $2
		)`, materializerRiverQueued, now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			dispatched_at, dispatched_transport, dispatched_route_generation,
			transport_job_id, created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'post_sync',
			'dispatched', $2, 1, $2, 'celery', 1, 'post-job', $2, $2
		)`, materializerPostSyncExists, now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
}

func seedRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, status string, createdAt time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_runs (id, org_id, status, created_at)
		VALUES ($1, 'org-materializer', $2, $3)`, id, status, createdAt); err != nil {
		t.Fatal(err)
	}
}

func seedUnit(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	id, runID, status string,
	availableAt *time.Time,
	updatedAt time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_units (id, sync_run_id, status, available_at, updated_at)
		VALUES ($1, $2, $3, $4, $5)`,
		id, runID, status, availableAt, updatedAt); err != nil {
		t.Fatal(err)
	}
}

func seedDiscoveryLedger(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	runID, status string,
	availableAt time.Time,
	leaseExpiresAt *time.Time,
	_ int,
	_ *string,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_reference_discoveries (
			sync_run_id, status, available_at, lease_expires_at
		) VALUES ($1, $2, $3, $4)`,
		runID, status, availableAt, leaseExpiresAt); err != nil {
		t.Fatal(err)
	}
}

// seedMaterializerDiscoveryDispatchedOutbox seeds a reference_discovery
// outbox row in the exact shape markRiverDispatchedSQL leaves behind after a
// normal successful publish -- every claim field NULL, status 'dispatched'
// via 'river'. This is NOT the "live/still-claimed" shape the
// materializerDiscovery fixture in seedMaterializerIntegrationGraph covers;
// it is the shape a River job leaves once delivery itself completed, whether
// or not the underlying discovery work it triggered ever ran to a
// terminal outcome.
func seedMaterializerDiscoveryDispatchedOutbox(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	runID, jobID string,
	dispatchedAt time.Time,
	attempts int,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			dispatched_at, dispatched_transport, dispatched_route_generation,
			transport_job_id, created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'reference_discovery',
			'dispatched', $2, $3, $2, 'river', 1, $4, $2, $2
		)`,
		runID, dispatchedAt, attempts, jobID); err != nil {
		t.Fatal(err)
	}
}

func assertMaterializerDiscoveryOutboxState(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	runID, wantStatus string,
	wantAttempts int,
) {
	t.Helper()
	var status string
	var attempts int
	if err := pool.QueryRow(ctx, `
		SELECT status, attempts
		FROM public.sync_dispatch_outbox
		WHERE sync_run_id = $1 AND kind = 'reference_discovery'`,
		runID,
	).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || attempts != wantAttempts {
		t.Fatalf("discovery outbox state for %s = %s/%d, want %s/%d",
			runID, status, attempts, wantStatus, wantAttempts)
	}
}

func seedMaterializerDispatchedOutbox(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	runID, jobID string,
	dispatchedAt time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			dispatched_at, dispatched_transport, dispatched_route_generation,
			transport_job_id, created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'dispatch_sync_run',
			'dispatched', $2, 1, $2, 'river', 2, $3, $2, $2
		)`,
		runID, dispatchedAt, jobID); err != nil {
		t.Fatal(err)
	}
}

func seedMaterializerFeatureDisabledOutbox(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	runID string,
	dispatchedAt time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			last_error, dispatched_at, dispatched_transport,
			dispatched_route_generation, transport_job_id,
			created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'dispatch_sync_run',
			'dispatched', $2, 1, 'feature_disabled', $2, 'river', 2, 'feature-disabled-job', $2, $2
		)`,
		runID, dispatchedAt); err != nil {
		t.Fatal(err)
	}
}

func assertMaterializerDispatchState(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	runID, wantStatus string,
	wantTransport *string,
	wantAttempts int,
) {
	t.Helper()
	var (
		status    string
		transport *string
		attempts  int
	)
	if err := pool.QueryRow(ctx, `
		SELECT status, dispatched_transport, attempts
		FROM public.sync_dispatch_outbox
		WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run'`,
		runID,
	).Scan(&status, &transport, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || attempts != wantAttempts {
		t.Fatalf("dispatch state for %s = %s/%v/%d, want %s/%v/%d",
			runID, status, transport, attempts, wantStatus, wantTransport, wantAttempts)
	}
	if wantTransport == nil {
		if transport != nil {
			t.Fatalf("dispatch state for %s transport = %v, want nil", runID, transport)
		}
		return
	}
	if transport == nil || *transport != *wantTransport {
		t.Fatalf("dispatch state for %s transport = %v, want %s",
			runID, transport, *wantTransport)
	}
}

// seedMaterializerFinalizeDispatchedOutbox seeds a finalize_sync_run outbox
// row in the shape markRiverDispatchedSQL leaves behind, at a CHOSEN route
// generation -- the generation is a parameter because CHAOS-5462's live row
// carries a stale one (4) against a current river route at generation 2, and
// the materializer must refuse it without ever reading the route table.
func seedMaterializerFinalizeDispatchedOutbox(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	runID, jobID string,
	dispatchedAt time.Time,
	routeGeneration int64,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			dispatched_at, dispatched_transport, dispatched_route_generation,
			transport_job_id, created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'finalize_sync_run',
			'dispatched', $2, 1, $2, 'river', $4, $3, $2, $2
		)`,
		runID, dispatchedAt, jobID, routeGeneration); err != nil {
		t.Fatal(err)
	}
}

func assertMaterializerFinalizeState(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	runID, wantStatus string,
	wantTransport *string,
	wantGeneration int64,
) {
	t.Helper()
	var (
		status     string
		transport  *string
		generation *int64
	)
	if err := pool.QueryRow(ctx, `
		SELECT status, dispatched_transport, dispatched_route_generation
		FROM public.sync_dispatch_outbox
		WHERE sync_run_id = $1 AND kind = 'finalize_sync_run'`,
		runID,
	).Scan(&status, &transport, &generation); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus {
		t.Fatalf("finalize state for %s = %s, want %s", runID, status, wantStatus)
	}
	if wantTransport == nil {
		if transport != nil {
			t.Fatalf("finalize transport for %s = %v, want nil", runID, transport)
		}
	} else if transport == nil || *transport != *wantTransport {
		t.Fatalf("finalize transport for %s = %v, want %s", runID, transport, *wantTransport)
	}
	if generation == nil || *generation != wantGeneration {
		t.Fatalf("finalize route generation for %s = %v, want %d",
			runID, generation, wantGeneration)
	}
}

// assertMaterializerFinalizeIsReady is the non-vacuity control for the fence
// subtest: it evaluates the SHARED readiness contract
// (readyFinalizeDomainSQL, which is finalizeReadyRunPredicate verbatim) so a
// "the row was not re-armed" assertion cannot pass merely because the run was
// never a finalize candidate in the first place.
func assertMaterializerFinalizeIsReady(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	runID string,
	want bool,
) {
	t.Helper()
	var ready bool
	if err := pool.QueryRow(ctx, readyFinalizeDomainSQL, runID).Scan(&ready); err != nil {
		t.Fatal(err)
	}
	if ready != want {
		t.Fatalf("finalize readiness for %s = %t, want %t", runID, ready, want)
	}
}

func ptrTime(value time.Time) *time.Time {
	return &value
}

func ptrString(value string) *string {
	return &value
}

func assertMaterializerOutboxCount(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	runID, kind string,
	want int,
) {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM public.sync_dispatch_outbox
		WHERE sync_run_id = $1 AND kind = $2`, runID, kind).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("outbox count for %s/%s = %d, want %d", runID, kind, count, want)
	}
}

func leftPadMaterializerID(value int) string {
	return "00000000" + strconv.Itoa(value)
}

// seedMaterializerOutboxWithAttempts writes a dispatch wakeup that has already
// been claimed and republished `attempts` times. That column is durable and
// already persisted; nothing new is instrumented to read it.
func seedMaterializerOutboxWithAttempts(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	runID string,
	attempts int64,
	at time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'dispatch_sync_run',
			'pending', $3, $2, $3, $3
		)`, runID, attempts, at); err != nil {
		t.Fatal(err)
	}
}

// CHAOS-4097 item 2. One production sync_dispatch_outbox row reached
// attempts = 72601, generating roughly 1500 no-op River jobs a minute for
// twenty-two hours, and nothing anywhere said a word about it.
//
// The report is what makes that loud. It changes no predicate and no write --
// the LOOP is bounded by the sweep removing the stuck units the re-arm
// predicate keeps matching, not by this -- so every assertion here is about
// visibility, and the negative cases matter as much as the positive one: a
// threshold that fires on healthy rows would be turned off within a week and
// then this would be exactly as silent as it was before.
func TestMaterializerReportsRunawayDispatchWakeups(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := createMaterializerIntegrationFixture(ctx, pool); err != nil {
		t.Fatal(err)
	}
	materializer, err := NewMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	step := func() MaterializerResult {
		t.Helper()
		result, err := materializer.Step(ctx, now, now.Add(-15*time.Minute), 10)
		if err != nil {
			t.Fatalf("materializer step: %v", err)
		}
		return result
	}

	t.Run("a looping wakeup on a live run is reported", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		run := "00000000-0000-4000-8000-" + leftPadMaterializerID(9001)
		seedRun(t, ctx, pool, run, "dispatching", now.Add(-24*time.Hour))
		seedMaterializerOutboxWithAttempts(t, ctx, pool, run, 72601, now.Add(-time.Hour))

		result := step()
		if len(result.Runaway) != 1 || result.RunawayTruncated || result.RunawayTotal != 1 {
			t.Fatalf("runaway report = %#v total=%d, want exactly the looping row",
				result.Runaway, result.RunawayTotal)
		}
		if result.Runaway[0].SyncRunID != run || result.Runaway[0].Attempts != 72601 {
			t.Fatalf("runaway row = %#v, want the run and its durable attempt count",
				result.Runaway[0])
		}
	})

	// NON-VACUITY, and the reason the threshold is not lower. Every healthy
	// dispatch_sync_run row in production sat at a p99 of 43-72 attempts; a
	// report that fired there would be noise, and noise gets muted.
	t.Run("an ordinary attempt count is not reported", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		run := "00000000-0000-4000-8000-" + leftPadMaterializerID(9002)
		seedRun(t, ctx, pool, run, "dispatching", now.Add(-time.Hour))
		seedMaterializerOutboxWithAttempts(t, ctx, pool, run,
			runawayDispatchAttempts-1, now.Add(-time.Hour))

		if result := step(); len(result.Runaway) != 0 || result.RunawayTotal != 0 {
			t.Fatalf("runaway report = %#v total=%d, want a healthy row left unreported",
				result.Runaway, result.RunawayTotal)
		}
	})

	// A finished run's row is inert: its count is archaeology, not an
	// operational signal. Production holds 3397 completed rows that would
	// otherwise be re-reported on every single pass, forever.
	t.Run("a terminal run is not reported however high its count", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		run := "00000000-0000-4000-8000-" + leftPadMaterializerID(9003)
		seedRun(t, ctx, pool, run, "success", now.Add(-48*time.Hour))
		seedMaterializerOutboxWithAttempts(t, ctx, pool, run, 72601, now.Add(-time.Hour))

		if result := step(); len(result.Runaway) != 0 || result.RunawayTotal != 0 {
			t.Fatalf("runaway report = %#v total=%d, want a finished run left unreported",
				result.Runaway, result.RunawayTotal)
		}
	})

	// A widespread degradation must not turn one pass into an unbounded burst
	// of log lines -- and the cap must SAY it capped. A silently truncated
	// report reads as "these are all of them", which is the same quiet
	// under-reporting this whole ticket is about.
	t.Run("the report is capped and says so", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		for index := 0; index < runawayDispatchScan+3; index++ {
			run := "00000000-0000-4000-8000-" + leftPadMaterializerID(9100+index)
			seedRun(t, ctx, pool, run, "dispatching", now.Add(-24*time.Hour))
			seedMaterializerOutboxWithAttempts(t, ctx, pool, run,
				int64(runawayDispatchAttempts+index), now.Add(-time.Hour))
		}

		result := step()
		if len(result.Runaway) != runawayDispatchScan || !result.RunawayTruncated {
			t.Fatalf("runaway report = %d rows truncated=%t, want %d and a truncation flag",
				len(result.Runaway), result.RunawayTruncated, runawayDispatchScan)
		}
		// THE EXACT TOTAL, against real SQL. The sample is capped; the metric
		// is not, and count(*) OVER () is what makes that true. A unit test
		// with a fake cannot prove the window function sees the whole filtered
		// set rather than the LIMITed page -- only Postgres can.
		if result.RunawayTotal != int64(runawayDispatchScan+3) {
			t.Fatalf("RunawayTotal = %d, want the exact %d seeded rows; a gauge fed "+
				"from the capped sample would report %d and understate the incident",
				result.RunawayTotal, runawayDispatchScan+3, len(result.Runaway))
		}
		// Worst first, so a capped report is still the most useful rows.
		if result.Runaway[0].Attempts <= result.Runaway[len(result.Runaway)-1].Attempts {
			t.Fatalf("runaway report is not ordered worst-first: %#v", result.Runaway)
		}
	})
}
