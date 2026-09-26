//go:build integration

package syncreconciler

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-6890, the second half of the 08:14-08:23Z burst: ONE run's outbox row
// was claimed and published ~1 time a second for nine minutes (delivery_attempt
// 7 170, 480 River jobs in nine minutes, every job completing on attempt 1).
//
// The dispatch materializer re-arms a `dispatched` river row while the run holds
// a `dispatching` unit older than the stale cutoff. The unit's own clock
// (updated_at) only moves when a dispatch job EXECUTES and reclaims the unit. The
// relay publishes the re-armed row within a tick, so the row is `dispatched`
// again one tick later while the job it just published is still queued behind
// busy workers -- and the unit is still stale, so the next tick re-arms it again.
// The 'planned' disjunct has a gate on the row's own dispatched_at for exactly
// this reason (CHAOS-4357 round 2: "publish a SECOND dispatch job for the same
// run, every tick, until the first finally executed"); the 'dispatching' and
// 'retrying' disjuncts have none.
//
// TestMaterializerRedispatchesStaleUnitsExactlyOnce steps the materializer
// twice with NO relay publish in between, so the row is still `pending` on the
// second step and the loop cannot show; this test closes the loop the way the
// kernel does.
func TestMaterializerDoesNotRepublishAStaleUnitsRunEveryTickWhileItsJobIsQueued(t *testing.T) {
	const ticks = 30
	for _, tc := range []struct {
		name         string
		unitStatus   string
		jobRunsAfter int // ticks after which the published job executes and reclaims the unit (0 = never runs)
		maxRearms    int
	}{
		// The healthy case: the job starts within a tick or two and refreshes the
		// unit, which ends the loop. Must pass on any code.
		{name: "the published job runs promptly", unitStatus: "dispatching", jobRunsAfter: 2, maxRearms: 3},
		// Workers busy or wedged (prod 08:14Z: eight slots parked in advisory-lock
		// waits): the job stays queued. One re-arm per stale window is the design.
		{name: "the published job stays queued", unitStatus: "dispatching", jobRunsAfter: 0, maxRearms: 1},
		// The retrying disjunct has the same shape: its clock (available_at) only moves when a
		// job executes and claims the due unit.
		{name: "a due retrying unit and the published job stays queued", unitStatus: "retrying", jobRunsAfter: 0, maxRearms: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			instance, err := containers.StartPostgres(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer closeCancel()
				_ = instance.Close(closeCtx)
			}()
			pool, err := pgxpool.New(ctx, instance.URI)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			if err := createMaterializerIntegrationFixture(ctx, t, pool); err != nil {
				t.Fatal(err)
			}
			materializer, err := NewMaterializer(pool)
			if err != nil {
				t.Fatal(err)
			}

			const runID = "00000000-0000-4000-8000-000000004a01"
			const unitID = "00000000-0000-4000-8000-000000004a02"
			start := time.Date(2026, time.September, 26, 8, 14, 0, 0, time.UTC)
			seedRun(t, ctx, pool, runID, "running", start.Add(-2*time.Hour))
			var availableAt *time.Time
			if tc.unitStatus == "retrying" {
				due := start.Add(-time.Hour)
				availableAt = &due
			}
			seedUnit(t, ctx, pool, unitID, runID, tc.unitStatus, availableAt, start.Add(-time.Hour))
			seedMaterializerDispatchedOutbox(t, ctx, pool, runID, "river-stale-job", start.Add(-2*time.Hour))

			var rearms int
			var publishedAtTick = -1
			for tick := 0; tick < ticks; tick++ {
				now := start.Add(time.Duration(tick) * time.Second)
				result, err := materializer.Step(ctx, now, now.Add(-15*time.Minute), 20)
				if err != nil {
					t.Fatal(err)
				}
				rearms += int(result.Dispatch)
				// The relay (kernel) claims and publishes the re-armed row within its
				// own one-second tick: the row is `dispatched` again before the next
				// materializer step.
				published, err := pool.Exec(ctx, `
UPDATE public.sync_dispatch_outbox
SET status='dispatched', attempts=attempts+1, dispatched_at=$2, dispatched_transport='river',
    dispatched_route_generation=2, transport_job_id='river-job-' || (attempts+1)::text,
    claim_token=NULL, claim_expires_at=NULL, claim_transport=NULL, claim_route_generation=NULL, updated_at=$2
WHERE sync_run_id=$1 AND kind='dispatch_sync_run' AND status='pending'`, runID, now)
				if err != nil {
					t.Fatal(err)
				}
				if published.RowsAffected() == 1 && publishedAtTick < 0 {
					publishedAtTick = tick
				}
				// The dispatch job, when workers are free, reclaims the stale unit.
				if tc.jobRunsAfter > 0 && publishedAtTick >= 0 && tick == publishedAtTick+tc.jobRunsAfter {
					if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET updated_at=$2 WHERE id=$1`, unitID, now); err != nil {
						t.Fatal(err)
					}
				}
			}
			if rearms > tc.maxRearms {
				t.Fatalf("the materializer re-armed the run %d times in %d one-second ticks (want <= %d): a stale "+
					"`dispatching` unit is only refreshed when a dispatch job executes, so while the job it "+
					"published is queued every tick re-armed the row again, the relay published again, and the "+
					"queue grew one job per second per such run (CHAOS-6890, prod delivery_attempt 7 170)",
					rearms, ticks, tc.maxRearms)
			}
		})
	}
}
