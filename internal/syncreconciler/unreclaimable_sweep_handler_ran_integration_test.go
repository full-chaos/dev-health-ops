//go:build integration

package syncreconciler

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
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
			// The reason names the class of the job's last error, never its text.
			_, _, _, payload := sweepUnitState(t, ctx, pool, unit)
			if !strings.Contains(*reason, `last error class "retryable"`) || strings.Contains(*reason, "dev-health job failed") {
				t.Fatalf("unit reason = %q, want the error CLASS (retryable) and none of the error text", *reason)
			}
			var decoded map[string]string
			if err := json.Unmarshal(payload, &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded["river_job_last_error_class"] != "retryable" || strings.Contains(string(payload), "dev-health job failed") {
				t.Fatalf("result payload = %s, want river_job_last_error_class and no error text", payload)
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
			name:      "the outbox delivery budget remains (StrandRepair's to re-arm, never scanned by the sweep)",
			heartbeat: old,
			delivery: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unit string) {
				seedSweepDeliveryWithBudget(t, ctx, pool, unit, "discarded", "dev-health job failed [retryable]", 5, 5, 1)
			},
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
			if status, _, _, _ := sweepUnitState(t, ctx, pool, unit); status != "dispatching" || result.Terminalized != 0 || result.Candidates != 0 {
				t.Fatalf("unit status = %q, result %+v: an attempted unit must be left alone unless its delivery is "+
					"dead, its budget spent and its worker silent", status, result)
			}
			if result.DeferredToRepair != tc.wantDeferred {
				t.Fatalf("DeferredToRepair = %d, want %d (%+v)", result.DeferredToRepair, tc.wantDeferred, result)
			}
		})
	}
}

// r1 P1 (CHAOS-6890): the candidate scan is bounded (unreclaimableMaximumScan rows, cursor
// restarting every pass). An attempted unit with no outbox row can never be acted on, so if
// the candidate SQL admits it, a prefix of them older than a genuinely dead delivery uses up
// the scan budget every pass and hides that delivery for ever.
func TestUnreclaimableSweepIsNotStarvedByAnAttemptedPrefixWithNoDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_units (
			id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status,
			attempts, last_heartbeat_at, created_at, updated_at)
		SELECT gen_random_uuid(), $1, $2, $3::uuid, $4::uuid, 'github', 'repo-metadata', 'heavy', 'incremental',
			'dispatching', 2, $5::timestamptz, $6::timestamptz - make_interval(secs => n), $7::timestamptz
		FROM generate_series(1, $8::int) AS n`,
		sweepOrg, sweepRun, pgseed.DefaultSyncIntegrationID, pgseed.DefaultSyncSourceID,
		now.Add(-33*time.Hour), now.Add(-40*time.Hour), now.Add(-90*time.Minute), unreclaimableMaximumScan); err != nil {
		t.Fatal(err)
	}
	target := sweepUnitID(72)
	spec := strandedSpec(target, "repo-metadata", "heavy", now) // created 16 h ago: after the whole prefix
	spec.attempts = 4
	heartbeat := now.Add(-33 * time.Hour)
	spec.heartbeat = &heartbeat
	seedSweepUnit(t, ctx, pool, spec)
	seedSweepDelivery(t, ctx, pool, target, "discarded", "dev-health job failed [retryable]")

	result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if status, _, _, _ := sweepUnitState(t, ctx, pool, target); status != "failed" || result.Terminalized != 1 {
		t.Fatalf("target status = %q, result %+v: %d older attempted units with no outbox row used up the sweep's scan budget "+
			"and hid a provably dead delivery (r1 P1)", status, result, unreclaimableMaximumScan)
	}
}

// r2 P1 (CHAOS-6890): the bounded candidate scan must not be spent on attempted units that
// StrandRepair owns. 1 000 older attempted units with a delivered row, a HEALTHY queued River
// job and outbox budget left (the reviewer's shape) used up the sweep's scan budget every pass
// and hid a genuinely dead attempted unit whose outbox budget is spent.
func TestUnreclaimableSweepIsNotStarvedByAttemptedUnitsStrandRepairOwns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
	heartbeat := now.Add(-33 * time.Hour)
	for i := 0; i < unreclaimableMaximumScan; i++ {
		unit := sweepUnitID(1000 + i)
		spec := strandedSpec(unit, "repo-metadata", "heavy", now)
		spec.createdAt = now.Add(-40*time.Hour - time.Duration(i)*time.Second) // all older than the target
		spec.attempts = 2
		spec.heartbeat = &heartbeat
		seedSweepUnit(t, ctx, pool, spec)
		seedSweepDeliveryWithBudget(t, ctx, pool, unit, "available", "", 1, 5, 1)
	}
	target := sweepUnitID(72)
	spec := strandedSpec(target, "repo-metadata", "heavy", now) // created 16 h ago: after the whole prefix
	spec.attempts = 4
	spec.heartbeat = &heartbeat
	seedSweepUnit(t, ctx, pool, spec)
	seedSweepDelivery(t, ctx, pool, target, "discarded", "dev-health job failed [retryable]")

	result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if status, _, _, _ := sweepUnitState(t, ctx, pool, target); status != "failed" || result.Terminalized != 1 {
		t.Fatalf("target status = %q, result %+v: %d attempted units with a live queued job and outbox budget left "+
			"(StrandRepair's, never the sweep's) used up the scan budget and hid a dead delivery (r2 P1)",
			status, result, unreclaimableMaximumScan)
	}
}
