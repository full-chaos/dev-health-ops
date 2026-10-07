//go:build integration

package workerservice

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

// dispatchNightlyRun stages the nightly run of one organization, as the fixed
// schedule does, and dispatches it through the rig's dispatcher.
func dispatchNightlyRun(t *testing.T, ctx context.Context, rig *touchedRig, orgID string, due time.Time) daily.Run {
	t.Helper()
	run := rig.startRun(t, ctx, func(tx pgx.Tx) (daily.Run, error) {
		return rig.store.StartScheduledFanoutRunTx(ctx, tx, daily.ScheduledFanoutRequest{
			OrganizationID: orgID, TargetDay: due,
			Generation: daily.ScheduledFanoutGenerationPrefix + due.Format(time.RFC3339),
		}, nilPartitionPublisher{})
	})
	execution := &jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs]{
		OrganizationID: &orgID,
		Envelope:       jobcontract.Envelope{OrganizationID: &orgID, Domain: jobcontract.DomainLink{Type: "daily_metrics_run", ID: run.ID}},
		Args: jobruntime.DailyMetricsDispatchArgs{EnvelopeArgs: func() jobruntime.EnvelopeArgs[jobcontract.DailyMetricsDispatchPayload] {
			args := nilPartitionExecution[jobcontract.DailyMetricsDispatchPayload](orgID, "daily_metrics_run", run.ID)
			args.Payload = jobcontract.DailyMetricsDispatchPayload{RunID: run.ID}
			return args
		}()},
	}
	if err := rig.dispatch.Work(ctx, execution); err != nil {
		t.Fatalf("dispatch of the nightly run: %v", err)
	}
	return run
}

// openDrainRuns returns the days and one run id of the drain runs of the
// organization that are not ended.
func openDrainRuns(t *testing.T, ctx context.Context, rig *touchedRig, orgID string) (days []string, runID string) {
	t.Helper()
	rows, err := rig.pool.Query(ctx, `
SELECT id::text, target_day::text FROM public.daily_metrics_runs
WHERE org_id = $1::uuid AND generation LIKE 'touched-drain:%' AND status IN ('pending', 'running')
ORDER BY target_day`, orgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var day string
		if err := rows.Scan(&runID, &day); err != nil {
			t.Fatal(err)
		}
		days = append(days, day)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return days, runID
}

// endDrainRuns writes the terminal state of every open drain run of the
// organization, as the finalize job of each run does.
func endDrainRuns(t *testing.T, ctx context.Context, rig *touchedRig, orgID, status string) {
	t.Helper()
	if _, err := rig.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET status = $2, finalization_status = $2, finalized_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE org_id = $1::uuid AND generation LIKE 'touched-drain:%' AND status IN ('pending', 'running')`, orgID, status); err != nil {
		t.Fatal(err)
	}
}

// seedPendingDays writes one work item for each of count days before newest
// and records the days as touched, with no fan-out: the state of an
// organization after a sync whose fan-out left the days pending. It returns
// the days, oldest first.
func seedPendingDays(t *testing.T, ctx context.Context, rig *touchedRig, orgID string, newest time.Time, count int) []string {
	t.Helper()
	now := time.Now().UTC()
	var items []touchedItem
	var days []string
	for offset := 0; offset < count; offset++ {
		day := newest.AddDate(0, 0, -offset)
		days = append(days, day.Format("2006-01-02"))
		items = append(items, touchedItem{repo: uuid.Nil, id: fmt.Sprintf("linear:OPS-%d", offset), provider: "linear", day: day, synced: now})
	}
	sort.Strings(days)
	insertTouchedItems(t, ctx, rig.conn, orgID, items...)
	if _, err := rig.touched.RecordTouched(ctx, orgID, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	return days
}

// An organization has 100 pending touched days and no later sync. The nightly
// run and the end of each batch of runs are the only events. Every day gets a
// run, the newest days first, 31 for each pass, and no day stays pending: with
// no new touches the oldest days are reached too.
func TestAnOrganizationWithPendingTouchedDaysAndNoLaterSyncGetsEveryDayARun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	rig := newTouchedRig(t, ctx)
	afterRunEnd := wireTouchedDaysDrain(t, rig)
	orgID := uuid.NewString()
	days := seedPendingDays(t, ctx, rig, orgID, time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), 100)
	if got := rig.pendingDays(t, ctx, orgID); !reflect.DeepEqual(got, days) {
		t.Fatalf("pending before the nightly run = %d days, want 100", len(got))
	}

	dispatchNightlyRun(t, ctx, rig, orgID, time.Date(2026, 8, 20, 1, 0, 0, 0, time.UTC))

	var passes [][]string
	for pass := 0; pass < 10; pass++ {
		batch, runID := openDrainRuns(t, ctx, rig, orgID)
		if len(batch) == 0 {
			break
		}
		passes = append(passes, batch)
		endDrainRuns(t, ctx, rig, orgID, "succeeded")
		afterRunEnd(ctx, orgID, runID)
	}
	want := [][]string{days[69:], days[38:69], days[7:38], days[:7]}
	if !reflect.DeepEqual(passes, want) {
		sizes := make([]int, 0, len(passes))
		for _, batch := range passes {
			sizes = append(sizes, len(batch))
		}
		t.Fatalf("drain passes started runs for %v days (first pass %v); want 31, 31, 31, 7 days, newest first", sizes, firstOf(passes))
	}
	if got := rig.pendingDays(t, ctx, orgID); len(got) != 0 {
		t.Fatalf("%d days are pending after the drain, want none: %v", len(got), got)
	}
}

func firstOf(passes [][]string) []string {
	if len(passes) == 0 {
		return nil
	}
	return passes[0]
}
