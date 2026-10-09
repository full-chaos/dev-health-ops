//go:build integration

package workerservice

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// The drain judges a mark against the take time that its run recorded, so a
// missing mark is known and a key touched again after the take is not one.
// These tests run the production store, run adapter and fan-out; only the mark
// is made to fail (the wrappers of the other drain tests) where a case needs it.

const (
	drainEventTakeAbsent    = jobruntime.TouchedDaysDrainEvent("take_time_absent")
	drainEventTakeUnwritten = jobruntime.TouchedDaysDrainEvent("take_time_unwritten")
)

// takeTimes reads the take time of every run of the organization whose
// generation starts with prefix, by run id. A run with none maps to the zero
// time.
func takeTimes(t *testing.T, ctx context.Context, rig *touchedRig, orgID, prefix string) map[string]time.Time {
	t.Helper()
	rows, err := rig.pool.Query(ctx, `
SELECT id::text, touched_take_at FROM public.daily_metrics_runs
WHERE org_id = $1::uuid AND generation LIKE $2`, orgID, prefix+"%")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	times := map[string]time.Time{}
	for rows.Next() {
		var (
			id string
			at *time.Time
		)
		if err := rows.Scan(&id, &at); err != nil {
			t.Fatal(err)
		}
		times[id] = time.Time{}
		if at != nil {
			times[id] = at.UTC()
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return times
}

// dispatchedAtMillis is the time of the newest 'dispatched' event of one key.
func dispatchedAtMillis(t *testing.T, ctx context.Context, rig *touchedRig, orgID string, day, repo string) int64 {
	t.Helper()
	var millis int64
	if err := rig.conn.QueryRow(ctx, `
SELECT toUnixTimestamp64Milli(max(at)) FROM daily_metrics_touched_days
WHERE org_id = ? AND toString(day) = ? AND toString(repo_id) = ? AND kind = 'dispatched'`,
		orgID, day, repo).Scan(&millis); err != nil {
		t.Fatal(err)
	}
	return millis
}

// insertNightlyRun adds an ended run of the scheduled fan-out of the
// organization: a run that lists no key and marks none.
func insertNightlyRun(t *testing.T, ctx context.Context, rig *touchedRig, orgID string, day time.Time) string {
	t.Helper()
	runID := uuid.NewString()
	if _, err := rig.pool.Exec(ctx, `
INSERT INTO public.daily_metrics_runs
    (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at, finalized_at, full_org)
VALUES ($1::uuid, $2::uuid, $3::date, $4, 'succeeded', 'succeeded', clock_timestamp(), clock_timestamp(), clock_timestamp(), true)`,
		runID, orgID, day.Format("2006-01-02"), "fixed-schedule:daily_metrics_fanout:"+uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	return runID
}

// The take time is written with the runs of a pass, and the mark of the pass
// carries it: the keys are dispatched one millisecond before it.
func TestTouchedDaysDrainRecordsTheTakeTimeWithItsRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID := uuid.NewString()
	newest := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	days := seedPendingDays(t, ctx, rig.touchedRig, orgID, newest, 3)

	rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, pass("n"))

	times := takeTimes(t, ctx, rig.touchedRig, orgID, daily.TouchedDrainGenerationPrefix)
	if len(times) != len(days) {
		t.Fatalf("drain runs = %d, want %d", len(times), len(days))
	}
	var first time.Time
	for id, at := range times {
		if at.IsZero() {
			t.Fatalf("run %s has no take time: a run of the drain must record it", id)
		}
		if first.IsZero() {
			first = at
		}
		if !at.Equal(first) {
			t.Fatalf("take times of one pass differ: %s and %s", first, at)
		}
	}
	if now := time.Now().UTC(); first.After(now) || now.Sub(first) > time.Hour {
		t.Fatalf("take time %s is not the time of this pass (now %s)", first, now)
	}
	for _, day := range days {
		if got, want := dispatchedAtMillis(t, ctx, rig.touchedRig, orgID, day, uuid.Nil.String()), first.UnixMilli()-1; got != want {
			t.Fatalf("day %s dispatched at %d, want the take time minus one millisecond (%d)", day, got, want)
		}
	}
}

// A fan-out records one take time on all of its runs: the window runs and the
// runs of the touched days, for rows of every provider shape.
func TestPostSyncFanoutRecordsTheTakeTimeOnEveryRunOfTheSyncRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID := uuid.NewString()
	target := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	repoA, repoB := uuid.New(), uuid.New()
	insertTouchedRepo(t, ctx, rig.conn, orgID, repoA, "acme/api", "github")
	insertTouchedRepo(t, ctx, rig.conn, orgID, repoB, "acme/web", "gitlab")
	now := time.Now().UTC()
	insertTouchedItems(t, ctx, rig.conn, orgID,
		touchedItem{repo: repoA, id: "gh:acme/api#1", provider: "github", day: target.AddDate(0, 0, -3), completed: true, synced: now},
		touchedItem{repo: repoB, id: "gitlab:acme/web#1", provider: "gitlab", day: target.AddDate(0, 0, -4), completed: true, synced: now},
		touchedItem{repo: uuid.Nil, id: "jira:OPS-1", provider: "jira", day: target.AddDate(0, 0, -5), completed: true, synced: now},
		touchedItem{repo: uuid.Nil, id: "linear:OPS-2", provider: "linear", day: target.AddDate(0, 0, -6), completed: true, synced: now},
	)
	args := rig.seedSync(t, ctx, orgID, "work-items", target)
	if err := rig.service(t, rig.touched, nil).Fanout(ctx, args); err != nil {
		t.Fatal(err)
	}

	runs := rig.runsOf(t, ctx, orgID, args)
	times := takeTimes(t, ctx, rig.touchedRig, orgID, "post-sync:"+args.RunID)
	if len(times) != len(runs) || len(times) < 5 {
		t.Fatalf("take times of %d runs, fan-out started %d, want at least 5 (the window run and 4 touched days)", len(times), len(runs))
	}
	var first time.Time
	for id, at := range times {
		if at.IsZero() {
			t.Fatalf("run %s of the fan-out has no take time", id)
		}
		if first.IsZero() {
			first = at
		}
		if !at.Equal(first) {
			t.Fatalf("take times of one fan-out differ: %s and %s", first, at)
		}
	}
	for _, day := range []int{3, 4, 5, 6} {
		key := target.AddDate(0, 0, -day).Format("2006-01-02")
		if _, ok := runs[key]; !ok {
			t.Fatalf("no run for the touched day %s: %v", key, touchedRunDays(runs))
		}
	}
}

// While the mark does not reach ClickHouse, the end of any run stops the chain
// once: a run of the fan-out, a run of the drain, and the run of the scheduled
// fan-out (which marks nothing itself) all trigger a pass that starts nothing.
func TestTouchedDaysDrainEveryRunEndStopsWhileAMarkIsMissing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	target := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	perPass := syncdispatchruntime.TouchedDaysPerDrainPass

	// seed leaves 41 days pending behind a fan-out whose mark failed.
	seed := func(t *testing.T) (orgID string, fanoutRunIDs []string) {
		t.Helper()
		rig.reset()
		orgID = uuid.NewString()
		var items []touchedItem
		now := time.Now().UTC()
		for offset := 1; offset <= 40; offset++ {
			items = append(items, touchedItem{repo: uuid.Nil, id: fmt.Sprintf("linear:OPS-%d", offset), provider: "linear",
				day: target.AddDate(0, 0, -20-offset), synced: now})
		}
		insertTouchedItems(t, ctx, rig.conn, orgID, items...)
		args := rig.seedSync(t, ctx, orgID, "work-items", target)
		failing := touchedFaultStore{TouchedDaysStore: rig.touched, failMark: true}
		if err := rig.service(t, failing, nil).Fanout(ctx, args); err != nil {
			t.Fatal(err)
		}
		for _, run := range rig.runsOf(t, ctx, orgID, args) {
			fanoutRunIDs = append(fanoutRunIDs, run.id)
		}
		if _, err := rig.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs SET status = 'succeeded', finalization_status = 'succeeded', finalized_at = clock_timestamp()
WHERE org_id = $1::uuid`, orgID); err != nil {
			t.Fatal(err)
		}
		// The 40 touched days and the window day of the sync run.
		if got := len(rig.pendingDays(t, ctx, orgID)); got != 41 {
			t.Fatalf("pending after the fan-out whose mark failed = %d days, want 41", got)
		}
		return orgID, fanoutRunIDs
	}

	t.Run("the end of a run of the fan-out", func(t *testing.T) {
		orgID, runIDs := seed(t)
		before := totalRuns(drainRunsOf(t, ctx, rig, orgID))
		rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, "e:"+runIDs[0])
		if got := totalRuns(drainRunsOf(t, ctx, rig, orgID)) - before; got != 0 {
			t.Fatalf("the pass after the end of a fan-out run started %d runs, want 0: the mark of the fan-out is missing", got)
		}
		if got := rig.observer.count(drainEventChainStopped); got != 1 {
			t.Fatalf("chain_stopped_mark_missing = %d, want 1", got)
		}
	})

	t.Run("the end of the run of the scheduled fan-out", func(t *testing.T) {
		orgID, _ := seed(t)
		nightly := insertNightlyRun(t, ctx, rig.touchedRig, orgID, target)
		rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, "e:"+nightly)
		if got := totalRuns(drainRunsOf(t, ctx, rig, orgID)); got != 0 {
			t.Fatalf("the pass after the end of the scheduled run started %d runs, want 0", got)
		}
		if got := rig.observer.count(drainEventChainStopped); got != 1 {
			t.Fatalf("chain_stopped_mark_missing = %d, want 1", got)
		}
	})

	t.Run("the end of a run of the drain, and the run after it", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		days := seedPendingDays(t, ctx, rig.touchedRig, orgID, target, 40)
		failing := rig.drain(t, drainFaultStore{TouchedDaysDrainStore: rig.touched, failMark: true}, nil)
		failing.DrainTouchedDays(ctx, orgID, pass("n"))
		if got := totalRuns(drainRunsOf(t, ctx, rig, orgID)); got != perPass {
			t.Fatalf("runs of the first pass = %d, want %d", got, perPass)
		}
		_, ended := openDrainRuns(t, ctx, rig.touchedRig, orgID)
		endDrainRuns(t, ctx, rig.touchedRig, orgID, "succeeded")
		nightly := insertNightlyRun(t, ctx, rig.touchedRig, orgID, target)
		for _, trigger := range []string{"e:" + ended, "e:" + nightly, "e:" + ended, "e:" + nightly} {
			rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, trigger)
		}
		if got := totalRuns(drainRunsOf(t, ctx, rig, orgID)); got != perPass {
			t.Fatalf("drain runs after 4 run ends = %d, want %d: each end started the newest days again", got, perPass)
		}
		if got := len(rig.pendingDays(t, ctx, orgID)); got != len(days) {
			t.Fatalf("pending = %d days, want %d", got, len(days))
		}
	})
}

// Keys touched again while the runs ran are pending after the mark. That is
// normal after a sync with a wide window, and the chain goes on: the touched
// days get their next runs.
func TestTouchedDaysDrainKeysTouchedAgainAfterTheTakeDoNotStopTheChain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID := uuid.NewString()
	newest := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	days := seedPendingDays(t, ctx, rig.touchedRig, orgID, newest, 3)
	// A sync touches every key of the pass again after it read the pending days
	// and before it marked them: the whole of the pass's run is pending.
	touchedAgain := drainFaultStore{TouchedDaysDrainStore: rig.touched, beforeMark: func() {
		now, err := rig.touched.RecordTouched(ctx, orgID, time.Now().UTC().Add(-time.Hour), nil)
		if err != nil || now == 0 {
			t.Errorf("touch again: recorded %d keys, err %v", now, err)
		}
	}}
	rig.drain(t, touchedAgain, nil).DrainTouchedDays(ctx, orgID, pass("n"))
	if got := rig.pendingDays(t, ctx, orgID); !reflect.DeepEqual(got, days) {
		t.Fatalf("pending after the pass = %v, want every day (all touched again after the take)", got)
	}
	_, ended := openDrainRuns(t, ctx, rig.touchedRig, orgID)
	endDrainRuns(t, ctx, rig.touchedRig, orgID, "succeeded")

	rig.reset()
	rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, "e:"+ended)

	if got := rig.observer.count(drainEventChainStopped); got != 0 {
		t.Fatalf("chain_stopped_mark_missing = %d, want 0: the keys were touched again after the take, the mark did land", got)
	}
	counts := drainRunsOf(t, ctx, rig, orgID)
	for _, day := range days {
		if counts[day] != 2 {
			t.Fatalf("drain runs of %s = %d, want 2 (the day touched again gets its next run): %v", day, counts[day], counts)
		}
	}
}

// A key is a missing mark only when it was last touched strictly before the
// take time. A touch at exactly the take time is after the mark (which stamps
// the keys one millisecond before it) and is not.
func TestTouchedDaysDrainATouchAtTheTakeTimeIsNotAMissingMark(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID := uuid.NewString()
	newest := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	days := seedPendingDays(t, ctx, rig.touchedRig, orgID, newest, 1)
	rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, pass("n"))
	_, ended := openDrainRuns(t, ctx, rig.touchedRig, orgID)
	endDrainRuns(t, ctx, rig.touchedRig, orgID, "succeeded")
	var take time.Time
	for _, at := range takeTimes(t, ctx, rig.touchedRig, orgID, daily.TouchedDrainGenerationPrefix) {
		take = at
	}
	touchedEvent(t, ctx, rig.touchedRig, orgID, newest, uuid.Nil.String(), "touched", take)
	if got := rig.pendingDays(t, ctx, orgID); !reflect.DeepEqual(got, days) {
		t.Fatalf("pending = %v, want %v: a touch at the take time is after the mark", got, days)
	}

	rig.reset()
	rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, "e:"+ended)
	if got := rig.observer.count(drainEventChainStopped); got != 0 {
		t.Fatalf("chain_stopped_mark_missing = %d, want 0 for a touch at the take time", got)
	}

	// One millisecond earlier it is a key the mark should have ended: the mark
	// would have stamped it dispatched. It is pending only because the mark
	// never landed.
	other := uuid.NewString()
	touchedEvent(t, ctx, rig.touchedRig, orgID, newest, other, "touched", take.Add(-time.Millisecond))
	runs, _, err := rig.productionRuns().store.TouchedMarkingRunsToCheck(ctx, orgID, ended, 24*time.Hour, 10)
	if err != nil || len(runs) != 1 {
		t.Fatalf("marking runs to check = %v, err %v", runs, err)
	}
	checked := runs[0]
	checked.RepositoryIDs = append(checked.RepositoryIDs, other)
	got, err := rig.touched.DaysPendingSinceBeforeTake(ctx, orgID, []syncdispatchruntime.TouchedRunKeys{syncdispatchruntime.TouchedRunKeys(checked)})
	if err != nil || len(got) != 1 {
		t.Fatalf("days pending since before the take = %v, err %v: a touch one millisecond before the take time is a missing mark", got, err)
	}
}

// A run that has no take time (started by a build before the column) cannot be
// checked: the pass goes on, counts it and says so.
func TestTouchedDaysDrainARunWithoutATakeTimeDoesNotStopTheChain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID := uuid.NewString()
	newest := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	days := seedPendingDays(t, ctx, rig.touchedRig, orgID, newest, 2)
	now := time.Now().UTC()
	// A fan-out of an older build ran for the first day and its mark was lost:
	// the key is pending and the run has no take time.
	listedRun(t, ctx, rig.touchedRig, orgID, newest, "post-sync:"+uuid.NewString(), uuid.Nil.String(),
		now.Add(-time.Hour), "succeeded", now.Add(-50*time.Minute))
	var legacy string
	for id := range takeTimes(t, ctx, rig.touchedRig, orgID, "post-sync:") {
		legacy = id
	}

	rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, "e:"+legacy)

	if got := rig.observer.count(drainEventChainStopped); got != 0 {
		t.Fatalf("chain_stopped_mark_missing = %d, want 0: a run with no take time cannot be judged", got)
	}
	if got := rig.observer.count(drainEventTakeAbsent); got != 1 {
		t.Fatalf("take_time_absent = %d, want 1", got)
	}
	lines := rig.logLines("touched_days_drain.failed")
	if len(lines) != 1 || !strings.Contains(lines[0], "take_time_absent") {
		t.Fatalf("failure lines = %v, want one line of take_time_absent", lines)
	}
	if got := totalRuns(drainRunsOf(t, ctx, rig, orgID)); got != len(days) {
		t.Fatalf("drain runs = %d, want %d: the pass went on", got, len(days))
	}
}

// The runs of the days that are not skipped come first; the retries of skipped
// days only use the slots that are left. Retries of newer days never take the
// slots of older days that never failed.
func TestTouchedDaysDrainRetriesOnlyUseTheSlotsTheOtherDaysLeave(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID := uuid.NewString()
	newest := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	perPass := syncdispatchruntime.TouchedDaysPerDrainPass
	const retryDays, healthyDays = 35, 5
	days := seedPendingDays(t, ctx, rig.touchedRig, orgID, newest, retryDays+healthyDays) // oldest first
	now := time.Now().UTC()
	// The 35 newest days have 3 failed runs each, 30 hours old: all due for a retry.
	for _, day := range days[healthyDays:] {
		parsed, err := time.Parse("2006-01-02", day)
		if err != nil {
			t.Fatal(err)
		}
		for run := 0; run < syncdispatchruntime.TouchedDrainFailedRunsBeforeSkip; run++ {
			created := now.Add(-time.Duration(40-run) * time.Hour)
			listedRun(t, ctx, rig.touchedRig, orgID, parsed, daily.TouchedDrainGenerationPrefix+"e:"+uuid.NewString(), uuid.Nil.String(),
				created, "failed", created.Add(time.Minute))
		}
	}
	before := totalRuns(drainRunsOf(t, ctx, rig, orgID))

	rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, pass("n"))

	counts := drainRunsOf(t, ctx, rig, orgID)
	if got := totalRuns(counts) - before; got != perPass {
		t.Fatalf("the pass started %d runs, want %d", got, perPass)
	}
	for _, day := range days[:healthyDays] {
		if counts[day] != 1 {
			t.Fatalf("healthy day %s has %d runs, want 1: retries of newer days took its slot (%v)", day, counts[day], counts)
		}
	}
	retried := 0
	for _, day := range days[healthyDays:] {
		if counts[day] == syncdispatchruntime.TouchedDrainFailedRunsBeforeSkip+1 {
			retried++
		}
	}
	if retried != perPass-healthyDays {
		t.Fatalf("retried days = %d, want %d (the slots the healthy days left)", retried, perPass-healthyDays)
	}
	// The newest retries come first among the retries.
	newestRetried := days[len(days)-1]
	if counts[newestRetried] != syncdispatchruntime.TouchedDrainFailedRunsBeforeSkip+1 {
		t.Fatalf("the newest skipped day %s was not retried: %v", newestRetried, counts)
	}
}

// A database that has not the column yet (an older schema while this build
// runs) loses no run: the runs are committed, the miss is counted and logged,
// and the check of a run end reads the run as one of an unknown take time.
func TestTouchedDaysDrainRunsAreCommittedWhenTheTakeTimeColumnIsAbsent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	if _, err := rig.pool.Exec(ctx, `ALTER TABLE public.daily_metrics_runs DROP COLUMN touched_take_at`); err != nil {
		t.Fatal(err)
	}
	orgID := uuid.NewString()
	target := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	days := seedPendingDays(t, ctx, rig.touchedRig, orgID, target.AddDate(0, 0, -30), 2)

	rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, pass("n"))

	got := drainRunsOf(t, ctx, rig, orgID)
	if len(got) != len(days) {
		t.Fatalf("drain runs = %v, want one for each of %v: the missing column lost the runs", got, days)
	}
	if n := rig.observer.count(drainEventTakeUnwritten); n != 1 {
		t.Fatalf("take_time_unwritten = %d, want 1", n)
	}
	if n := rig.observer.count(jobruntime.TouchedDaysDrainPassFailed); n != 0 {
		t.Fatalf("pass_failed = %d, want 0", n)
	}
	if got := rig.pendingDays(t, ctx, orgID); len(got) != 0 {
		t.Fatalf("pending = %v, want none: the mark follows the runs", got)
	}
	_, ended := openDrainRuns(t, ctx, rig.touchedRig, orgID)
	endDrainRuns(t, ctx, rig.touchedRig, orgID, "succeeded")

	rig.reset()
	rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, "e:"+ended)
	if n := rig.observer.count(drainEventTakeAbsent); n != 1 {
		t.Fatalf("take_time_absent = %d, want 1 (the run end reads the run as one of an unknown take)", n)
	}
	if n := rig.observer.count(jobruntime.TouchedDaysDrainPassFailed); n != 0 {
		t.Fatalf("pass_failed after the run end = %d, want 0", n)
	}

	// The fan-out too: it commits its runs without the column.
	args := rig.seedSync(t, ctx, orgID, "work-items", target)
	if err := rig.service(t, rig.touched, nil).Fanout(ctx, args); err != nil {
		t.Fatalf("fan-out without the column: %v", err)
	}
	if runs := rig.runsOf(t, ctx, orgID, args); len(runs) == 0 {
		t.Fatal("the fan-out started no run")
	}
}

// A run of every repository lists every key of its day: the same strict
// comparison applies to the oldest pending touch of that day.
func TestTouchedDaysDrainATouchAtTheTakeTimeOfARunOfEveryRepositoryIsNotAMissingMark(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID := uuid.NewString()
	day := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	touchedAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	touchedEvent(t, ctx, rig.touchedRig, orgID, day, uuid.NewString(), "touched", touchedAt)

	check := func(take time.Time) []time.Time {
		t.Helper()
		got, err := rig.touched.DaysPendingSinceBeforeTake(ctx, orgID, []syncdispatchruntime.TouchedRunKeys{{
			RunID: uuid.NewString(), Day: day, FullOrganization: true, TakenAt: take,
		}})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := check(touchedAt); len(got) != 0 {
		t.Fatalf("days pending since before the take = %v, want none: a touch at the take time is after the mark", got)
	}
	if got := check(touchedAt.Add(time.Millisecond)); len(got) != 1 {
		t.Fatalf("days pending since before the take = %v, want the day: a touch one millisecond before the take time is a missing mark", got)
	}
}

// The take time of a run is written once: a second stamp of the same
// generation does not move it.
func TestTouchedDaysDrainTheTakeTimeOfARunIsWrittenOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID := uuid.NewString()
	generation := daily.TouchedDrainGenerationPrefix + uuid.NewString()
	runID := uuid.NewString()
	if _, err := rig.pool.Exec(ctx, `
INSERT INTO public.daily_metrics_runs
    (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at, finalized_at, full_org)
VALUES ($1::uuid, $2::uuid, '2026-07-31'::date, $3, 'succeeded', 'succeeded', clock_timestamp(), clock_timestamp(), clock_timestamp(), true)`,
		runID, orgID, generation); err != nil {
		t.Fatal(err)
	}
	first := time.Date(2026, 7, 31, 10, 0, 0, 0, time.UTC)
	stamp := func(at time.Time) {
		t.Helper()
		tx, err := rig.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := rig.productionRuns().store.StampTouchedTakeTx(ctx, tx, orgID, generation, at); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	stamp(first)
	stamp(first.Add(time.Hour))
	if got := takeTimes(t, ctx, rig.touchedRig, orgID, daily.TouchedDrainGenerationPrefix)[runID]; !got.Equal(first) {
		t.Fatalf("take time = %v, want %v: a second stamp moved it", got, first)
	}
}

// insertMarkingRun adds an ended run of every repository of one day, created at
// the given time, with the status and take time the case needs (nil: none).
func insertMarkingRun(t *testing.T, ctx context.Context, rig *touchedRig, orgID string, day time.Time, status string, take *time.Time, created time.Time) string {
	t.Helper()
	runID := uuid.NewString()
	if _, err := rig.pool.Exec(ctx, `
INSERT INTO public.daily_metrics_runs
    (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at, finalized_at, full_org, touched_take_at)
VALUES ($1::uuid, $2::uuid, $3::date, $4, $5, $5, $7, $7, $7, true, $6)`,
		runID, orgID, day.Format("2006-01-02"), "post-sync:"+uuid.NewString(), status, take, created); err != nil {
		t.Fatal(err)
	}
	return runID
}

// Under the bound the newest runs are the ones checked: an unmarked key of the
// newest run stops the chain although more runs than the bound ended.
func TestTouchedDaysDrainStopCheckUnderTheBoundChecksTheNewestRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID := uuid.NewString()
	day := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	take := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	touchedEvent(t, ctx, rig.touchedRig, orgID, day, uuid.NewString(), "touched", take.Add(-time.Minute))
	insertMarkingRun(t, ctx, rig.touchedRig, orgID, day, "succeeded", &take, time.Now().UTC().Add(-time.Minute))
	for i := 1; i <= 204; i++ {
		insertMarkingRun(t, ctx, rig.touchedRig, orgID, day.AddDate(0, 0, -i), "succeeded", &take,
			time.Now().UTC().Add(-time.Duration(i+1)*time.Minute/2))
	}
	nightly := insertNightlyRun(t, ctx, rig.touchedRig, orgID, day.AddDate(0, 0, -400))
	rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, "e:"+nightly)
	if got := rig.observer.count(drainEventChainStopped); got != 1 {
		t.Fatalf("chain_stopped_mark_missing = %d, want 1: the missing mark of the newest run was not seen", got)
	}
	if got := rig.observer.count(jobruntime.TouchedDaysDrainStopCheckTruncated); got != 1 {
		t.Fatalf("stop_check_truncated = %d, want 1", got)
	}
}

// Runs without a take time do not use the slots of the bound: during a rolling
// deploy many of them must not hide a stamped run behind them.
func TestTouchedDaysDrainRunsWithoutATakeTimeDoNotUseTheSlotsOfTheBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID := uuid.NewString()
	day := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	take := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	touchedEvent(t, ctx, rig.touchedRig, orgID, day, uuid.NewString(), "touched", take.Add(-time.Minute))
	insertMarkingRun(t, ctx, rig.touchedRig, orgID, day, "succeeded", &take, time.Now().UTC().Add(-time.Hour))
	for i := 1; i <= 205; i++ {
		insertMarkingRun(t, ctx, rig.touchedRig, orgID, day.AddDate(0, 0, -i), "succeeded", nil,
			time.Now().UTC().Add(-time.Duration(i)*time.Second))
	}
	nightly := insertNightlyRun(t, ctx, rig.touchedRig, orgID, day.AddDate(0, 0, -400))
	rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, "e:"+nightly)
	if got := rig.observer.count(drainEventChainStopped); got != 1 {
		t.Fatalf("chain_stopped_mark_missing = %d, want 1: runs without a take time hid the stamped run", got)
	}
}

// A run that ended failed or canceled is checked too: its keys were marked
// only by a mark that landed, so a lost mark stops the chain.
func TestTouchedDaysDrainAFailedRunWithALostMarkStopsTheChain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID := uuid.NewString()
	day := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	take := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	touchedEvent(t, ctx, rig.touchedRig, orgID, day, uuid.NewString(), "touched", take.Add(-time.Minute))
	insertMarkingRun(t, ctx, rig.touchedRig, orgID, day, "failed", &take, time.Now().UTC().Add(-time.Minute))
	nightly := insertNightlyRun(t, ctx, rig.touchedRig, orgID, day.AddDate(0, 0, -400))
	rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, "e:"+nightly)
	if got := rig.observer.count(drainEventChainStopped); got != 1 {
		t.Fatalf("chain_stopped_mark_missing = %d, want 1 for a failed run with a lost mark", got)
	}
}

// The run whose end triggered the pass is read apart from the newest runs, so
// no number of newer ended runs hides its lost mark.
func TestTouchedDaysDrainTheTriggerRunIsCheckedWhateverNumberOfNewerRunsEnded(t *testing.T) {
	for _, newer := range []int{199, 200, 201, 260} {
		t.Run(fmt.Sprintf("%d_newer_runs", newer), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			rig := newDrainRig(t, ctx)
			orgID := uuid.NewString()
			day := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
			take := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
			touchedEvent(t, ctx, rig.touchedRig, orgID, day, uuid.NewString(), "touched", take.Add(-time.Minute))
			trigger := insertMarkingRun(t, ctx, rig.touchedRig, orgID, day, "succeeded", &take, time.Now().UTC().Add(-3*time.Hour))
			for i := 1; i <= newer; i++ {
				insertMarkingRun(t, ctx, rig.touchedRig, orgID, day.AddDate(0, 0, -i), "succeeded", &take,
					time.Now().UTC().Add(-time.Duration(i)*time.Second))
			}
			rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, "e:"+trigger)
			if got := rig.observer.count(drainEventChainStopped); got != 1 {
				t.Fatalf("chain_stopped_mark_missing = %d, want 1 with %d newer ended runs: the lost mark of the trigger run was not seen", got, newer)
			}
			stopped := false
			for _, line := range rig.logLines("touched_days_drain.failed") {
				if strings.Contains(line, "chain_stopped_mark_missing") {
					stopped = true
					if !strings.Contains(line, `"drain_trigger_checked":true`) {
						t.Fatalf("the stop line does not say that the trigger run was checked: %s", line)
					}
				}
			}
			if !stopped {
				t.Fatal("no chain_stopped_mark_missing line")
			}
		})
	}
}

// The trigger run is returned once even when it is among the newest runs, and
// it takes no slot of the limit: the flag says truncated only when a run is
// really left out.
func TestTouchedDaysDrainTheTriggerRunAmongTheNewestIsReturnedOnceAndTakesNoSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID := uuid.NewString()
	day := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	take := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	trigger := insertMarkingRun(t, ctx, rig.touchedRig, orgID, day, "succeeded", &take, time.Now().UTC().Add(-time.Minute))
	others := map[string]bool{}
	for i := 1; i <= 3; i++ {
		id := insertMarkingRun(t, ctx, rig.touchedRig, orgID, day.AddDate(0, 0, -i), "succeeded", &take,
			time.Now().UTC().Add(-time.Duration(i+1)*time.Minute))
		others[id] = true
	}
	store := rig.productionRuns().store
	runs, truncated, err := store.TouchedMarkingRunsToCheck(ctx, orgID, trigger, 24*time.Hour, 3)
	if err != nil {
		t.Fatal(err)
	}
	if truncated {
		t.Fatal("truncated = true, want false: no run is left out (the trigger run takes no slot of the limit)")
	}
	if len(runs) != 4 || runs[0].RunID != trigger {
		t.Fatalf("runs = %v, want the trigger run first and the 3 others", runs)
	}
	for _, run := range runs[1:] {
		if !others[run.RunID] {
			t.Fatalf("run %s is not one of the others (the trigger run was returned twice?): %v", run.RunID, runs)
		}
	}
	runs, truncated, err = store.TouchedMarkingRunsToCheck(ctx, orgID, trigger, 24*time.Hour, 2)
	if err != nil || !truncated || len(runs) != 3 {
		t.Fatalf("limit 2: runs %d, truncated %v, err %v; want 3 runs (trigger + 2) and truncated", len(runs), truncated, err)
	}
}
