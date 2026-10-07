//go:build integration

package workerservice

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// drainBarrierStore holds each pass after its read of the pending
// repositories until every pass of the test has made that read: all of them
// then hold the same picture of the pending days.
type drainBarrierStore struct {
	syncdispatchruntime.TouchedDaysDrainStore
	arrived *sync.WaitGroup
}

func (store drainBarrierStore) PendingRepositories(ctx context.Context, org string, days []time.Time, limit int) (map[string][]string, error) {
	repositories, err := store.TouchedDaysDrainStore.PendingRepositories(ctx, org, days, limit)
	store.arrived.Done()
	store.arrived.Wait()
	return repositories, err
}

// drainEndBeforeSecondLockRuns ends the run of the first pass, with the given
// status, at the moment the second pass counts the runs in flight under its
// lock. The second pass then finds no run in flight.
type drainEndBeforeSecondLockRuns struct {
	syncdispatchruntime.TouchedDaysDrainRuns
	status string
	calls  *drainCallCount
}

type drainCallCount struct {
	mu    sync.Mutex
	calls int
}

func (runs drainEndBeforeSecondLockRuns) InFlightTx(ctx context.Context, tx pgx.Tx, org string, since time.Time) (int, error) {
	runs.calls.mu.Lock()
	runs.calls.calls++
	call := runs.calls.calls
	runs.calls.mu.Unlock()
	// Calls 1 and 2 are the first looks of the two passes, call 3 is the
	// count of the first pass under its lock, call 4 the one of the second.
	if call == 4 {
		tag, err := tx.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET status = $2, finalization_status = $2, finalized_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE org_id = $1::uuid AND generation LIKE 'touched-drain:%' AND status IN ('pending', 'running')`, org, runs.status)
		if err != nil {
			return 0, err
		}
		if tag.RowsAffected() != 1 {
			return 0, fmt.Errorf("the run of the first pass was not in flight at the count of the second: updated %d rows", tag.RowsAffected())
		}
	}
	return runs.TouchedDaysDrainRuns.InFlightTx(ctx, tx, org, since)
}

// Two passes of one organization that read the same pending days start the
// runs of those days once: the second one reads the state of the runs again
// under its lock and starts nothing. The run of the first pass has ended when
// the second one counts the runs in flight, so that count does not stop it.
func TestTouchedDaysDrainTwoPassesAtOnceStartTheRunOfADayOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	day := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	dayKey := day.Format("2006-01-02")

	cases := []struct {
		name string
		// skipped is true for a day whose newest runs all failed and whose
		// newest run is older than a day: the day is due for one more run.
		skipped bool
		// endAs is the end of the run of the first pass.
		endAs string
		// coordinators is the number of drains the two passes run in.
		coordinators int
	}{
		{name: "a skipped day that is due gets one retry run", skipped: true, endAs: "failed", coordinators: 1},
		{name: "a skipped day that is due gets one retry run from two coordinators", skipped: true, endAs: "failed", coordinators: 2},
		{name: "a pending day gets one run when the first run has a result already", endAs: "succeeded", coordinators: 1},
		{name: "a pending day gets one run from two coordinators", endAs: "succeeded", coordinators: 2},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			rig.reset()
			orgID, repo := uuid.NewString(), uuid.NewString()
			now := time.Now().UTC().Truncate(time.Millisecond)
			touchedEvent(t, ctx, rig.touchedRig, orgID, day, repo, "touched", now.Add(-72*time.Hour))
			if test.skipped {
				for index := 0; index < syncdispatchruntime.TouchedDrainFailedRunsBeforeSkip; index++ {
					created := now.Add(-time.Duration(30-index) * time.Hour)
					listedRun(t, ctx, rig.touchedRig, orgID, day, "post-sync:"+uuid.NewString(), repo,
						created, "failed", created.Add(time.Minute))
				}
				due, err := rig.store.TouchedDaysWithOnlyFailedRuns(ctx, orgID, []time.Time{day},
					syncdispatchruntime.TouchedDrainFailedRunsBeforeSkip, 24*time.Hour, 24*time.Hour)
				if err != nil || !due[dayKey] {
					t.Fatalf("retry of the day = %v (%v), want due before the passes start", due, err)
				}
			}
			arrived := &sync.WaitGroup{}
			arrived.Add(2)
			calls := &drainCallCount{}
			drains := make([]*syncdispatchruntime.TouchedDaysDrain, 0, test.coordinators)
			for index := 0; index < test.coordinators; index++ {
				drains = append(drains, rig.drain(t,
					drainBarrierStore{TouchedDaysDrainStore: rig.touched, arrived: arrived},
					drainEndBeforeSecondLockRuns{TouchedDaysDrainRuns: rig.productionRuns(), status: test.endAs, calls: calls}))
			}

			var passes sync.WaitGroup
			passes.Add(2)
			for index := 0; index < 2; index++ {
				drain := drains[index%len(drains)]
				go func() {
					defer passes.Done()
					drain.DrainTouchedDays(ctx, orgID, pass("n"))
				}()
			}
			passes.Wait()

			if calls.calls != 4 {
				t.Fatalf("counts of the runs in flight = %d, want 4: the second pass did not reach its lock", calls.calls)
			}
			if got := drainRunsOf(t, ctx, rig, orgID)[dayKey]; got != 1 {
				t.Fatalf("two passes started %d runs for one day, want 1", got)
			}
			if got := rig.observer.count(jobruntime.TouchedDaysDrainPassFailed); got != 0 {
				t.Fatalf("failed passes = %d, want 0: %s", got, rig.logs.String())
			}
			var changed int
			for _, line := range rig.logLines("touched_days_drain.pass") {
				if strings.Contains(line, "runs_changed_since_read") {
					changed++
				}
			}
			if changed != 1 {
				t.Fatalf("pass lines with the outcome runs_changed_since_read = %d, want 1: %s", changed, rig.logs.String())
			}
		})
	}
}

// drainFailingReturnStore fails the return of the keys of the runs without a
// result.
type drainFailingReturnStore struct {
	syncdispatchruntime.TouchedDaysDrainStore
}

func (drainFailingReturnStore) ReturnToPending(context.Context, string, []syncdispatchruntime.TouchedRunKeys) (int, error) {
	return 0, syncdispatchruntime.ErrTouchedDaysUnavailable
}

// A pass that hits the bound of the read of the runs without a result says so
// also when a later step of it fails: the runs behind the bound have their
// line and their count whatever the end of the pass.
func TestTouchedDaysDrainReportsAFullReadOfTheRunsWithoutAResultWhenThePassFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID := uuid.NewString()
	// One failed drain run for each of 3661 days, each with one repository.
	if _, err := rig.pool.Exec(ctx, `
WITH runs AS (
    INSERT INTO public.daily_metrics_runs
        (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at, full_org)
    SELECT gen_random_uuid(), $1::uuid, DATE '2026-07-31' - step, 'touched-drain:e:' || gen_random_uuid()::text,
           'failed', 'failed', clock_timestamp() - make_interval(hours => step), clock_timestamp(), false
    FROM generate_series(1, 3661) AS step
    RETURNING id
)
INSERT INTO public.daily_metrics_partitions (id, run_id, ordinal, repo_ids, status, attempt_count, created_at, updated_at)
SELECT gen_random_uuid(), id, 0, json_build_array(gen_random_uuid()::text), 'failed', 0, clock_timestamp(), clock_timestamp()
FROM runs`, orgID); err != nil {
		t.Fatal(err)
	}

	rig.drain(t, drainFailingReturnStore{TouchedDaysDrainStore: rig.touched}, nil).DrainTouchedDays(ctx, orgID, pass("n"))

	if got := rig.observer.count(jobruntime.TouchedDaysDrainPassFailed); got != 1 {
		t.Fatalf("failed passes = %d, want 1: the return of the keys fails in this test", got)
	}
	if got := rig.observer.count(jobruntime.TouchedDaysDrainReturnReadTruncated); got != 1 {
		t.Fatalf("return_read_truncated = %d, want 1: the pass failed after it hit the bound", got)
	}
	var lines []string
	for _, line := range rig.logLines("touched_days_drain.failed") {
		if strings.Contains(line, "return_read_truncated") && strings.Contains(line, `"level":"ERROR"`) {
			lines = append(lines, line)
		}
	}
	if len(lines) != 1 {
		t.Fatalf("Error lines of the full read = %v, want 1", lines)
	}
}
