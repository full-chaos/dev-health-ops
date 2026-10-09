//go:build integration

package workerservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
)

// drainObserver collects the counters and the pending ages of drain passes.
type drainObserver struct {
	mu     sync.Mutex
	counts map[jobruntime.TouchedDaysDrainEvent]uint64
	ages   map[string]time.Duration
}

func (observer *drainObserver) ObserveTouchedDaysDrain(event jobruntime.TouchedDaysDrainEvent, count uint64) error {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.counts == nil {
		observer.counts = map[jobruntime.TouchedDaysDrainEvent]uint64{}
	}
	observer.counts[event] += count
	return nil
}

func (observer *drainObserver) ObserveTouchedDaysOldestPendingAge(organizationID string, age time.Duration) error {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.ages == nil {
		observer.ages = map[string]time.Duration{}
	}
	observer.ages[organizationID] = age
	return nil
}

func (observer *drainObserver) count(event jobruntime.TouchedDaysDrainEvent) uint64 {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return observer.counts[event]
}

// drainFaultStore is the real ClickHouse store with one step made to fail or
// to call back.
type drainFaultStore struct {
	syncdispatchruntime.TouchedDaysDrainStore
	failMark   bool
	beforeMark func()
}

func (store drainFaultStore) MarkDispatched(ctx context.Context, org string, at time.Time, days []time.Time, keys []syncdispatchruntime.TouchedDayKey) error {
	if store.beforeMark != nil {
		store.beforeMark()
	}
	if store.failMark {
		return syncdispatchruntime.ErrTouchedDaysUnavailable
	}
	return store.TouchedDaysDrainStore.MarkDispatched(ctx, org, at, days, keys)
}

// drainFaultRuns is the production run adapter with a lower repository limit
// or a start that fails.
type drainFaultRuns struct {
	touchedDrainRuns
	limit       int
	failStartAt int
	starts      *int
}

func (runs drainFaultRuns) RepositoryLimit() int {
	if runs.limit > 0 {
		return runs.limit
	}
	return runs.touchedDrainRuns.RepositoryLimit()
}

func (runs drainFaultRuns) StartTx(ctx context.Context, tx pgx.Tx, org string, day time.Time, passID string, repositories []string) (bool, error) {
	if runs.starts != nil {
		*runs.starts++
		if *runs.starts == runs.failStartAt {
			return false, errors.New("the process stopped between the take and the start")
		}
	}
	return runs.touchedDrainRuns.StartTx(ctx, tx, org, day, passID, repositories)
}

type drainRig struct {
	*touchedRig
	observer *drainObserver
	logs     *bytes.Buffer
}

func (rig *drainRig) productionRuns() touchedDrainRuns {
	return touchedDrainRuns{store: rig.store, publisher: nilPartitionPublisher{}}
}

// drain builds a drain over the given store and runs (nil = production).
func (rig *drainRig) drain(t *testing.T, store syncdispatchruntime.TouchedDaysDrainStore, runs syncdispatchruntime.TouchedDaysDrainRuns) *syncdispatchruntime.TouchedDaysDrain {
	t.Helper()
	if store == nil {
		store = rig.touched
	}
	if runs == nil {
		runs = rig.productionRuns()
	}
	drain, err := syncdispatchruntime.NewTouchedDaysDrain(
		rig.pool, store, runs, synclog.New(slog.New(slog.NewJSONHandler(rig.logs, nil))))
	if err != nil {
		t.Fatal(err)
	}
	drain.SetObserver(rig.observer)
	return drain
}

func (rig *drainRig) reset() {
	rig.observer = &drainObserver{}
	rig.logs.Reset()
}

func (rig *drainRig) logLines(message string) []string {
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(rig.logs.String()), "\n") {
		if strings.Contains(line, `"msg":"`+message+`"`) {
			lines = append(lines, line)
		}
	}
	return lines
}

// listedRunsByDay counts the runs of listed repositories (the runs of a
// fan-out's touched days and of the drain) of the organization, by day.
func (rig *drainRig) listedRunsByDay(t *testing.T, ctx context.Context, orgID string) map[string]int {
	t.Helper()
	rows, err := rig.pool.Query(ctx, `
SELECT target_day::text, count(*) FROM public.daily_metrics_runs
WHERE org_id = $1::uuid AND NOT full_org GROUP BY target_day`, orgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var day string
		var count int
		if err := rows.Scan(&day, &count); err != nil {
			t.Fatal(err)
		}
		counts[day] = count
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return counts
}

func (rig *drainRig) openDays(t *testing.T, ctx context.Context, orgID string) []string {
	t.Helper()
	days, _ := openDrainRuns(t, ctx, rig.touchedRig, orgID)
	return days
}

func pass(trigger string) string { return trigger + ":" + uuid.NewString() }

func TestTouchedDaysDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	rig := &drainRig{touchedRig: newTouchedRig(t, ctx), observer: &drainObserver{}, logs: &bytes.Buffer{}}
	target := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	newest := target.AddDate(0, 0, -20) // outside the window of a fan-out of target

	// A sync ends between two drain passes. Each of the two takes the newest
	// days that are pending when it reads: every day gets exactly one run.
	t.Run("a sync fan-out between two passes loses no day and no day runs twice", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		days := seedPendingDays(t, ctx, rig.touchedRig, orgID, newest, 80)
		drain := rig.drain(t, nil, nil)

		drain.DrainTouchedDays(ctx, orgID, pass("n"))
		if got := rig.openDays(t, ctx, orgID); !reflect.DeepEqual(got, days[49:]) {
			t.Fatalf("first pass started %d runs %v, want the 31 newest days", len(got), got)
		}
		args := rig.seedSync(t, ctx, orgID, "commits", target)
		if err := rig.service(t, rig.touched, nil).Fanout(ctx, args); err != nil {
			t.Fatal(err)
		}
		wantFanout := append(append([]string{}, days[18:49]...), target.Format("2006-01-02"))
		if got := touchedRunDays(rig.runsOf(t, ctx, orgID, args)); !reflect.DeepEqual(got, wantFanout) {
			t.Fatalf("the fan-out started %d runs %v, want the 31 newest pending days and the target day", len(got), got)
		}
		// Runs of the first pass are not ended: a trigger starts nothing.
		drain.DrainTouchedDays(ctx, orgID, pass("e"))
		if got := rig.openDays(t, ctx, orgID); len(got) != 31 {
			t.Fatalf("a pass started runs while %d drain runs were in flight: %d open now", 31, len(got))
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainInFlight); got != 1 {
			t.Fatalf("in_flight counter = %d, want 1", got)
		}
		endDrainRuns(t, ctx, rig.touchedRig, orgID, "succeeded")
		drain.DrainTouchedDays(ctx, orgID, pass("e"))
		if got := rig.openDays(t, ctx, orgID); !reflect.DeepEqual(got, days[:18]) {
			t.Fatalf("second pass started %d runs %v, want the 18 oldest days", len(got), got)
		}
		if got := rig.pendingDays(t, ctx, orgID); len(got) != 0 {
			t.Fatalf("pending after the passes = %v, want none", got)
		}
		counts := rig.listedRunsByDay(t, ctx, orgID)
		for _, day := range days {
			if counts[day] != 1 {
				t.Fatalf("day %s has %d runs, want exactly 1 (all: %v)", day, counts[day], counts)
			}
		}
	})

	// The accepted overlap: a fan-out that reads the record after the drain
	// committed its runs and before the drain marked them starts a second run
	// for the days both took. A day runs twice; no day is lost.
	t.Run("a fan-out in the seconds between the start and the mark computes a day twice and loses none", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		days := seedPendingDays(t, ctx, rig.touchedRig, orgID, newest, 40)
		args := rig.seedSync(t, ctx, orgID, "commits", target)
		store := drainFaultStore{TouchedDaysDrainStore: rig.touched, beforeMark: func() {
			if err := rig.service(t, rig.touched, nil).Fanout(ctx, args); err != nil {
				t.Errorf("fan-out: %v", err)
			}
		}}
		rig.drain(t, store, nil).DrainTouchedDays(ctx, orgID, pass("n"))
		counts := rig.listedRunsByDay(t, ctx, orgID)
		// Both paths took the 31 newest days = days[9:40]: each has two runs.
		for _, day := range days[9:] {
			if counts[day] != 2 {
				t.Fatalf("day %s has %d runs, want the 2 runs of the two paths (all: %v)", day, counts[day], counts)
			}
		}
		// The 9 older days were taken by neither and are still pending.
		if got := rig.pendingDays(t, ctx, orgID); !reflect.DeepEqual(got, days[:9]) {
			t.Fatalf("pending = %v, want the 9 oldest days %v: none lost, none marked without a run", got, days[:9])
		}
		for _, day := range days[:9] {
			if counts[day] != 0 {
				t.Fatalf("day %s has %d runs and is pending", day, counts[day])
			}
		}
	})

	t.Run("a stop between the take and the start leaves every day pending", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		days := seedPendingDays(t, ctx, rig.touchedRig, orgID, newest, 5)
		starts := 0
		runs := drainFaultRuns{touchedDrainRuns: rig.productionRuns(), failStartAt: 3, starts: &starts}
		rig.drain(t, nil, runs).DrainTouchedDays(ctx, orgID, pass("n"))
		if got := rig.listedRunsByDay(t, ctx, orgID); len(got) != 0 {
			t.Fatalf("runs after the rolled-back pass = %v, want none", got)
		}
		if got := rig.pendingDays(t, ctx, orgID); !reflect.DeepEqual(got, days) {
			t.Fatalf("pending after the rolled-back pass = %v, want all of %v", got, days)
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainPassFailed); got != 1 {
			t.Fatalf("pass_failed counter = %d, want 1", got)
		}
		if got := rig.logLines("touched_days_drain.failed"); len(got) != 1 || !strings.Contains(got[0], `"phase":"start"`) {
			t.Fatalf("Error lines = %v, want one of phase start", got)
		}
		rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, pass("e"))
		if got := rig.openDays(t, ctx, orgID); !reflect.DeepEqual(got, days) {
			t.Fatalf("the next pass started %v, want %v", got, days)
		}
	})

	t.Run("a stop between the start and the mark leaves the days pending and a second delivery starts nothing twice", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		days := seedPendingDays(t, ctx, rig.touchedRig, orgID, newest, 5)
		passID := pass("n")
		failing := rig.drain(t, drainFaultStore{TouchedDaysDrainStore: rig.touched, failMark: true}, nil)
		failing.DrainTouchedDays(ctx, orgID, passID)
		if got := rig.openDays(t, ctx, orgID); !reflect.DeepEqual(got, days) {
			t.Fatalf("runs of the pass = %v, want %v", got, days)
		}
		if got := rig.pendingDays(t, ctx, orgID); !reflect.DeepEqual(got, days) {
			t.Fatalf("pending after the failed mark = %v, want all of %v: a day would be lost", got, days)
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainMarkFailed); got != 1 {
			t.Fatalf("mark_failed counter = %d, want 1", got)
		}
		endDrainRuns(t, ctx, rig.touchedRig, orgID, "succeeded")
		// The same trigger again: its runs exist, so it starts none.
		rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, passID)
		if got := rig.listedRunsByDay(t, ctx, orgID); len(got) != 5 || got[days[0]] != 1 {
			t.Fatalf("runs after the second delivery = %v, want one for each of the 5 days", got)
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysAlreadyStarted); got != 5 {
			t.Fatalf("days_already_started counter = %d, want 5", got)
		}
		// The nightly trigger (the floor) computes the days once more and marks
		// them. A trigger that is the end of a run would stop here: the runs of
		// the pass ended and their mark is missing.
		rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, pass("n"))
		if got := rig.listedRunsByDay(t, ctx, orgID); got[days[0]] != 2 || got[days[4]] != 2 {
			t.Fatalf("runs after the next pass = %v, want a second run for each day", got)
		}
		if got := rig.pendingDays(t, ctx, orgID); len(got) != 0 {
			t.Fatalf("pending after the next pass = %v, want none", got)
		}
	})

	t.Run("two passes at the same time start each day once", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		days := seedPendingDays(t, ctx, rig.touchedRig, orgID, newest, 40)
		drain := rig.drain(t, nil, nil)
		var group sync.WaitGroup
		for index := 0; index < 2; index++ {
			group.Add(1)
			go func() {
				defer group.Done()
				drain.DrainTouchedDays(ctx, orgID, pass("e"))
			}()
		}
		group.Wait()
		if got := rig.openDays(t, ctx, orgID); !reflect.DeepEqual(got, days[9:]) {
			t.Fatalf("two passes started %d runs %v, want the 31 newest days once", len(got), got)
		}
		if started, inFlight := rig.observer.count(jobruntime.TouchedDaysDrainDaysStarted), rig.observer.count(jobruntime.TouchedDaysDrainInFlight); started != 31 || inFlight != 1 {
			t.Fatalf("days_started = %d, in_flight = %d; want 31 and 1", started, inFlight)
		}
	})

	// A day with more pending repositories than one run accepts is taken in
	// parts. After the last part the derived rows of the day equal a run of
	// every repository.
	t.Run("a day over the repository limit is split and equals a whole-day compute after the last part", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		day := newest
		now := time.Now().UTC()
		var repositories []string
		for index := 0; index < 5; index++ {
			repo := uuid.New()
			repositories = append(repositories, repo.String())
			insertTouchedRepo(t, ctx, rig.conn, orgID, repo, fmt.Sprintf("acme/split-%d", index), "github")
			insertTouchedItems(t, ctx, rig.conn, orgID, touchedItem{
				repo: repo, id: fmt.Sprintf("gh:acme/split-%d#1", index), provider: "github", day: day, completed: true, synced: now})
		}
		sort.Strings(repositories)
		if _, err := rig.touched.RecordTouched(ctx, orgID, now.Add(-time.Hour), nil); err != nil {
			t.Fatal(err)
		}
		drain := rig.drain(t, nil, drainFaultRuns{touchedDrainRuns: rig.productionRuns(), limit: 2})
		var taken []string
		var sizes []int
		for part := 0; part < 5; part++ {
			drain.DrainTouchedDays(ctx, orgID, pass("e"))
			_, runID := openDrainRuns(t, ctx, rig.touchedRig, orgID)
			if runID == "" {
				break
			}
			run, err := rig.store.LoadRun(ctx, runID)
			if err != nil {
				t.Fatal(err)
			}
			listed := 0
			for _, partition := range rig.dispatchAndRun(t, ctx, run) {
				for _, id := range partition.RepoIDs {
					taken = append(taken, string(id))
					listed++
				}
			}
			sizes = append(sizes, listed)
			endDrainRuns(t, ctx, rig.touchedRig, orgID, "succeeded")
		}
		sort.Strings(taken)
		if !reflect.DeepEqual(sizes, []int{2, 2, 1}) || !reflect.DeepEqual(taken, repositories) {
			t.Fatalf("parts of %v repositories, %d taken; want parts of 2, 2, 1 that hold each of the 5 repositories once", sizes, len(taken))
		}
		if got := rig.pendingDays(t, ctx, orgID); len(got) != 0 {
			t.Fatalf("pending after the last part = %v, want none", got)
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysSplit); got != 2 {
			t.Fatalf("days_split counter = %d, want 2", got)
		}
		if got := rig.logLines("touched_days_drain.day_split"); len(got) != 2 {
			t.Fatalf("split lines = %d, want 2: %v", len(got), got)
		}
		split := derivedDaySnapshot(t, ctx, rig.conn, orgID, day)
		rig.fullRecompute(t, ctx, orgID, day)
		if whole := derivedDaySnapshot(t, ctx, rig.conn, orgID, day); !reflect.DeepEqual(split, whole) {
			t.Fatalf("derived rows after the split parts differ from a run of every repository:\nsplit: %v\nwhole: %v", split, whole)
		}
		if strings.HasPrefix(split["work_item_metrics_daily"], "0 rows") {
			t.Fatalf("the split parts wrote no work_item_metrics_daily row: %v", split)
		}
	})

	// A run that ends failed must not lose its day, and a day that keeps
	// failing must not keep the drain busy or hold back other days.
	t.Run("a failed run returns its day to pending and a day whose runs keep failing is skipped", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		failing := newest
		failingKey := failing.Format("2006-01-02")
		now := time.Now().UTC()
		insertTouchedItems(t, ctx, rig.conn, orgID,
			touchedItem{repo: uuid.Nil, id: "linear:OPS-fail", provider: "linear", day: failing, synced: now})
		args := rig.seedSync(t, ctx, orgID, "work-items", target)
		if err := rig.service(t, rig.touched, nil).Fanout(ctx, args); err != nil {
			t.Fatal(err)
		}
		if got := rig.pendingDays(t, ctx, orgID); len(got) != 0 {
			t.Fatalf("pending after the fan-out = %v, want none (its run is started)", got)
		}
		failRunsOf := func(day string) {
			t.Helper()
			if _, err := rig.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET status = 'failed', finalization_status = 'failed', finalized_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE org_id = $1::uuid AND target_day = $2::date AND status IN ('pending', 'running')`, orgID, day); err != nil {
				t.Fatal(err)
			}
		}
		failRunsOf(failingKey) // the run of the fan-out ends failed
		drain := rig.drain(t, nil, nil)

		for attempt := 1; attempt <= 2; attempt++ {
			drain.DrainTouchedDays(ctx, orgID, pass("e"))
			if got := rig.openDays(t, ctx, orgID); !reflect.DeepEqual(got, []string{failingKey}) {
				t.Fatalf("pass %d after a failed run started %v, want a run of the failed day %s", attempt, got, failingKey)
			}
			failRunsOf(failingKey)
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysReturned); got != 2 {
			t.Fatalf("days_returned_to_pending counter = %d, want 2", got)
		}

		// Three runs of the day failed. Another day is pending too.
		other := newest.AddDate(0, 0, -7)
		otherKey := other.Format("2006-01-02")
		later := time.Now().UTC().Add(time.Minute)
		insertTouchedItems(t, ctx, rig.conn, orgID,
			touchedItem{repo: uuid.Nil, id: "linear:OPS-other", provider: "linear", day: other, synced: later})
		if _, err := rig.touched.RecordTouched(ctx, orgID, later.Add(-time.Second), nil); err != nil {
			t.Fatal(err)
		}
		rig.logs.Reset()
		drain.DrainTouchedDays(ctx, orgID, pass("e"))
		if got := rig.openDays(t, ctx, orgID); !reflect.DeepEqual(got, []string{otherKey}) {
			t.Fatalf("the pass after three failed runs started %v, want only the other day %s", got, otherKey)
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysSkipped); got != 1 {
			t.Fatalf("days_skipped_after_failed_runs counter = %d, want 1", got)
		}
		if got := rig.logLines("touched_days_drain.days_skipped_after_failed_runs"); len(got) != 1 || !strings.Contains(got[0], `"level":"ERROR"`) {
			t.Fatalf("skip lines = %v, want one Error line", got)
		}
		if got := rig.pendingDays(t, ctx, orgID); !reflect.DeepEqual(got, []string{failingKey}) {
			t.Fatalf("pending = %v, want the failed day %s to stay visible", got, failingKey)
		}
		if age := rig.observer.ages[orgID]; age <= 0 {
			t.Fatalf("oldest pending age = %v, want a positive age for the skipped day", age)
		}

		// The chain ends: the next pass starts no run, so no run end follows.
		endDrainRuns(t, ctx, rig.touchedRig, orgID, "succeeded")
		before := rig.listedRunsByDay(t, ctx, orgID)
		drain.DrainTouchedDays(ctx, orgID, pass("e"))
		if got := rig.openDays(t, ctx, orgID); len(got) != 0 {
			t.Fatalf("a pass started %v for a day whose runs keep failing", got)
		}
		if after := rig.listedRunsByDay(t, ctx, orgID); !reflect.DeepEqual(before, after) {
			t.Fatalf("runs changed in a pass that must start none: %v -> %v", before, after)
		}

		// A run of another trigger that succeeds makes the day startable.
		run := rig.startRun(t, ctx, func(tx pgx.Tx) (daily.Run, error) {
			return rig.store.StartRunTx(ctx, tx, daily.StartRunRequest{
				OrganizationID: orgID, TargetDay: failing, Generation: daily.ManualDailyGenerationPrefix + uuid.NewString()[:8],
				RepositoryIDs: []daily.RepositoryID{daily.RepositoryID(uuid.Nil.String())},
			}, nilPartitionPublisher{})
		})
		if _, err := rig.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs SET status = 'succeeded', finalization_status = 'succeeded', finalized_at = clock_timestamp()
WHERE id = $1::uuid`, run.ID); err != nil {
			t.Fatal(err)
		}
		drain.DrainTouchedDays(ctx, orgID, pass("e"))
		if got := rig.openDays(t, ctx, orgID); !reflect.DeepEqual(got, []string{failingKey}) {
			t.Fatalf("after a succeeded run of the day the pass started %v, want %s", got, failingKey)
		}
	})

	// A drain run that never ends must not stop the drain of its organization
	// for ever: after 24 hours the next trigger starts a pass again.
	t.Run("a drain run that never ends holds back the next pass for a day and no longer", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		days := seedPendingDays(t, ctx, rig.touchedRig, orgID, newest, 40)
		drain := rig.drain(t, nil, nil)
		drain.DrainTouchedDays(ctx, orgID, pass("n"))
		age := func(interval string) {
			t.Helper()
			if _, err := rig.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs SET created_at = clock_timestamp() - $2::interval
WHERE org_id = $1::uuid AND generation LIKE 'touched-drain:%'`, orgID, interval); err != nil {
				t.Fatal(err)
			}
		}
		age("23 hours")
		drain.DrainTouchedDays(ctx, orgID, pass("n"))
		if got := rig.openDays(t, ctx, orgID); !reflect.DeepEqual(got, days[9:]) {
			t.Fatalf("a pass started runs while runs of 23 hours were open: %d open, want the first 31", len(got))
		}
		age("25 hours")
		drain.DrainTouchedDays(ctx, orgID, pass("n"))
		// The runs that did not end in 24 hours are runs without a result:
		// their keys are pending again, and the pass is not held back. It
		// starts the newest days, which are the same 31; the 9 older days
		// wait for the pass after it.
		counts := drainRunsOf(t, ctx, rig, orgID)
		for index, key := range days {
			want := 2
			if index < 9 {
				want = 0
			}
			if counts[key] != want {
				t.Fatalf("with open runs of 25 hours the day %s has %d drain runs, want %d (runs by day: %v)", key, counts[key], want, counts)
			}
		}
	})

	t.Run("a pass reports the days it left and the age of the oldest pending day", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		days := seedPendingDays(t, ctx, rig.touchedRig, orgID, newest, 40)
		time.Sleep(1100 * time.Millisecond)
		drain := rig.drain(t, nil, nil)
		drain.DrainTouchedDays(ctx, orgID, pass("n"))
		lines := rig.logLines("touched_days_drain.pass")
		if len(lines) != 1 {
			t.Fatalf("pass lines = %d, want 1: %v", len(lines), lines)
		}
		for _, want := range []string{
			`"outcome":"started"`, `"drain_days_started":31`, `"drain_days_pending_left":9`,
			`"drain_oldest_pending_day":"` + days[0] + `T00:00:00Z"`, `"org_id":"` + orgID + `"`,
		} {
			if !strings.Contains(lines[0], want) {
				t.Fatalf("pass line lacks %s: %s", want, lines[0])
			}
		}
		if age := rig.observer.ages[orgID]; age < time.Second || age > time.Hour {
			t.Fatalf("oldest pending age = %v, want about the second the days waited", age)
		}
		endDrainRuns(t, ctx, rig.touchedRig, orgID, "succeeded")
		drain.DrainTouchedDays(ctx, orgID, pass("e"))
		endDrainRuns(t, ctx, rig.touchedRig, orgID, "succeeded")
		drain.DrainTouchedDays(ctx, orgID, pass("e"))
		if age, known := rig.observer.ages[orgID]; !known || age != 0 {
			t.Fatalf("oldest pending age after the drain = %v (known %v), want 0", age, known)
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainNothingPending); got != 1 {
			t.Fatalf("nothing_pending counter = %d, want 1", got)
		}
	})
}
