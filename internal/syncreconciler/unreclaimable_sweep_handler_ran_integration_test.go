//go:build integration

package syncreconciler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-6890, the executed cause. Prod run dbb92927's unit 22488b42 sat
// `dispatching` for 33 hours: github/blame, attempts 4 (a handler ran), last worker
// heartbeat 33 h old, no lease, its delivery `delivered` and its River job DEAD.
// orphaned_unit_repair counted it `skipped_other_repair` on 5 296 of 5 309 passes
// (the survey CASE sends 'discarded'/'cancelled' jobs to the other repairs), and
// every one of those "other repairs" -- joboutbox.TerminalDeliveryRepair,
// joboutbox.StrandRepair's provider shape and this sweep's destroy branch -- selects
// only `unit.attempts = 0`. A unit a handler ran even once is therefore owned by
// nobody: the dispatcher reclaims it every stale window, it holds a bucket slot and
// keeps its run open for ever.
//
// The proof a delivery is dead does not depend on whether the handler ran: the job
// is cancelled, or discarded with River's budget spent, the outbox delivery budget
// is spent, and no worker has heartbeaten the unit for a whole idle window.
func TestUnreclaimableSweepTerminalizesADeadDeliveryUnitAHandlerAlreadyRan(t *testing.T) {
	for _, tc := range []struct {
		name     string
		jobState string
	}{
		{"discarded with River's budget spent", "discarded"},
		{"cancelled", "cancelled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			pool := startSweepPostgres(t, ctx)
			now := time.Now().UTC()
			seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
			unit := sweepUnitID(70)
			spec := strandedSpec(unit, "repo-metadata", "heavy", now)
			spec.attempts = 4
			heartbeat := now.Add(-33 * time.Hour)
			spec.heartbeat = &heartbeat
			seedSweepUnit(t, ctx, pool, spec)
			seedSweepDelivery(t, ctx, pool, unit, tc.jobState, "dev-health job failed [retryable]")

			result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100)
			if err != nil {
				t.Fatalf("sweep: %v", err)
			}
			status, _, reason, _ := sweepUnitState(t, ctx, pool, unit)
			if status != "failed" || result.Terminalized != 1 {
				t.Fatalf("unit status = %q, sweep result %+v: a unit whose delivery is dead (%s, outbox budget "+
					"spent) and whose worker has been silent for 33 h must be failed with a reason even though a handler "+
					"ran it (attempts 4); left `dispatching` it holds a bucket slot and its run stays open for ever (CHAOS-6890)",
					status, result, tc.jobState)
			}
			if reason == nil || !strings.Contains(*reason, tc.jobState) {
				t.Fatalf("unit reason = %v, want it to name the River state %q", reason, tc.jobState)
			}
		})
	}
}

// The widened branch must stay narrow: each clause of "a handler ran it, but it is
// dead" is pinned by the case that lacks exactly that clause.
func TestUnreclaimableSweepSparesAnAttemptedUnitThatIsNotProvablyDead(t *testing.T) {
	old := func(now time.Time) *time.Time { v := now.Add(-33 * time.Hour); return &v }
	for _, tc := range []struct {
		name         string
		dataset      string // "" = repo-metadata (routable); "tests" is an alias identity the matrix declines
		heartbeat    func(now time.Time) *time.Time
		delivery     func(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unit string)
		wantDeferred int
	}{
		{
			name:      "a worker heartbeated a minute ago",
			heartbeat: func(now time.Time) *time.Time { v := now.Add(-time.Minute); return &v },
			delivery: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unit string) {
				seedSweepDelivery(t, ctx, pool, unit, "discarded", "dev-health job failed [retryable]")
			},
		},
		{
			name:      "no outbox row at all, on a pair the matrix declines",
			dataset:   "tests",
			heartbeat: old,
			delivery:  func(*testing.T, context.Context, *pgxpool.Pool, string) {},
		},
		{
			name:      "the River job is running",
			heartbeat: old,
			delivery: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unit string) {
				seedSweepDelivery(t, ctx, pool, unit, "running", "")
			},
		},
		{
			name:      "the River job is completed",
			heartbeat: old,
			delivery: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unit string) {
				seedSweepDelivery(t, ctx, pool, unit, "completed", "")
			},
		},
		{
			name:      "the outbox delivery budget remains (StrandRepair's to re-arm)",
			heartbeat: old,
			delivery: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unit string) {
				seedSweepDeliveryWithBudget(t, ctx, pool, unit, "discarded", "dev-health job failed [retryable]", 5, 5, 1)
			},
			wantDeferred: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			pool := startSweepPostgres(t, ctx)
			now := time.Now().UTC()
			seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
			unit := sweepUnitID(71)
			dataset := tc.dataset
			if dataset == "" {
				dataset = "repo-metadata"
			}
			spec := strandedSpec(unit, dataset, "heavy", now)
			spec.attempts = 4
			spec.heartbeat = tc.heartbeat(now)
			seedSweepUnit(t, ctx, pool, spec)
			tc.delivery(t, ctx, pool, unit)

			result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100)
			if err != nil {
				t.Fatalf("sweep: %v", err)
			}
			if status, _, _, _ := sweepUnitState(t, ctx, pool, unit); status != "dispatching" || result.Terminalized != 0 {
				t.Fatalf("unit status = %q, result %+v: an attempted unit must be left alone unless its delivery is "+
					"dead, its budget spent and its worker silent", status, result)
			}
			if result.DeferredToRepair != tc.wantDeferred {
				t.Fatalf("DeferredToRepair = %d, want %d (%+v)", result.DeferredToRepair, tc.wantDeferred, result)
			}
		})
	}
}
